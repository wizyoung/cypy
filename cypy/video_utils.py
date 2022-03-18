import os
import pprint
import random
import string
import cv2
from tqdm import tqdm
from collections import OrderedDict

from cypy.misc_utils import get_cmd_output, LazyImport, verbose_print
from cypy.logging_utils import EasyLoggerManager
from cypy.cli_utils import warn_print
from cypy.time_utils import Duration

import ffmpeg
import re
import mmap

# avoid conflict with yt_pyvideoreader
# from decord import VideoReader
decord = LazyImport('decord')

# TODO: use ffmpeg to convert and detect is better in the future
def detect_broken_duration_video(inp, format='file', check_tool='ffmpeg', convert=False, convert_params=None, progress=True, verbose=False, logger=None):
    assert format in ['file', 'list', 'txt'], 'format must be one of [file, list, txt], but got {}'.format(format)
    assert check_tool in ['ffmpeg', 'decord'], 'check_tool must be one of `ffmpeg` or `decord`, but got `{}`'.format(check_tool)

    if convert_params is None:
        convert_params = {
            'global_params': OrderedDict({"-v": "error"}),
            'input_params': OrderedDict(),
            'output_params': OrderedDict({
                '-c:v': 'libx264',
                '-pix_fmt': 'yuv420p',
                '-strict': '-2',
                '-max_muxing_queue_size': 9999
            })
        }
    else:
        # TODO: more restrictions
        assert isinstance(convert_params, dict)
        for key, val in convert_params.items():
            assert key in ['global_params', 'input_params', 'output_params'], f'convert_params must be a dict with keys "global_params", "input_params", and "output_params", your key {key} is illegal'
            assert isinstance(val, OrderedDict), f'values of convert_params must be type of OrderedDict as the order in ffmpeg is important, however, the value of key {key} is type {type(val)}'


    all_video_paths = []
    if format == 'file':
        all_video_paths = [inp]
    elif format == 'txt':
        with open(inp, 'r') as f:
            for line in f:
                video_path = line.strip()
                all_video_paths.append(video_path)
    else:
        all_video_paths = inp

    random_route = ''.join(random.sample(string.ascii_letters + string.digits, 8))
    tmp_vid_file_name = '/tmp/' + random_route + os.path.splitext(all_video_paths[0])[-1]
    
    output_normal = []  # normal videos with normal duration metadata
    output_abnormal = []  # abnormal videos with missing or broken duration metadata
    output_abnormal_converted_failed = []  # abnormal videos with missing or broken duration metadata and cannot be converted
    if logger is None:
        logger = EasyLoggerManager(random_route).get_logger(log_to_console=True, stream_handler_color=True, formatter_template=None, handler_singleton=True)

    if progress:
        all_video_paths = tqdm(all_video_paths)

    for idx, video_path in enumerate(all_video_paths):
        assert os.path.exists(video_path), f'The {idx}th item [{video_path}] does not exist'

        cur_abnormal_flag = False
        if check_tool == 'ffmpeg':
            info = get_video_info(video_path, force_decoding=False)
            if info.get('duration') in [-1, None]:
                cur_abnormal_flag = True
        else:
            try:
                vr = decord.VideoReader(video_path)
                img = vr[0].asnumpy()
                h, w, c = img.shape
                img = cv2.resize(img, (w//2, h//2))
            except:
                cur_abnormal_flag = True
        
        if cur_abnormal_flag:
            logger.warning(f"{video_path} duration is missing.")
            output_abnormal.append(video_path)
        
            if convert:
                cmd1 = 'ffmpeg -y'
                od = convert_params['global_params']
                if od:
                    cmd1 += ' ' + ' '.join(['{} {}'.format(k, v) for k, v in od.items()])
                od = convert_params['input_params']
                if od:
                    cmd1 += ' ' + ' '.join(['{} {}'.format(k, v) for k, v in od.items()])
                cmd1 += f' -i "{video_path}"'
                od = convert_params['output_params']
                if od:
                    cmd1 += ' ' + ' '.join(['{} {}'.format(k, v) for k, v in od.items()])
                cmd1 += f' {tmp_vid_file_name}'

                stdout, stderr = get_cmd_output(cmd1)
                verbose_print(stderr, verbose)
                # print(cmd1)

                if (not os.path.exists(tmp_vid_file_name)) or os.path.getsize(tmp_vid_file_name) == 0:
                    logger.error(f'{video_path} duration is missing and converted by ffmpeg failed.')
                    get_cmd_output(f'rm -rf {tmp_vid_file_name}')
                    output_abnormal_converted_failed.append(video_path)
                else:
                    cmd2 = f'mv {tmp_vid_file_name} "{video_path}"'
                    get_cmd_output(cmd2)
                    # print(cmd2)
        else:
            output_normal.append(video_path)

    return output_normal, output_abnormal, output_abnormal_converted_failed


def get_video_info(video_path, force_decoding=False):
    assert os.path.exists(video_path), f'{video_path} does not exist'

    # contains: duration(float), nb_frames(int), fps(float), height(int), width(int), rotation(int), original_height(int), original_width(int), codec_name(str), missing_fields(list)
    # -1 value means unknown or broken or missing
    # missing_fields: record missing fields in headers, mainly focusing on "tags" and "duration", for debug only.
    info_dict = {}
    info_dict['missing_fields'] = []

    # detailed ffprbe info reference: https://juejin.cn/post/6844903920750297101
    try:
        probe = ffmpeg.probe(video_path)
        format = probe['format']
        video_stream = next((stream for stream in probe['streams'] if stream['codec_type'] == 'video'), None)
    except Exception as e:
        warn_print(f'{video_path} not a valid video file, prase failed!')
        return info_dict

    if video_stream is None:
        warn_print(f'{video_path} is not a valid video file, maybe empty or corrupted')
        return info_dict
    
    if video_stream['codec_name'] in ['mjpeg', 'png', 'gif']:
        warn_print(f'{video_path} is not a valid video file, it is format of {video_stream["codec_name"]}')
        return info_dict

    fps = video_stream['avg_frame_rate']
    if fps == "0/0":
        fps = -1  # broken
    else:
        fps = float(eval(fps))
    nb_frames = video_stream.get('nb_frames', 0)
    if nb_frames == 0:
        nb_frames = -1
    else:
        nb_frames = int(nb_frames)

    # for WebM container with vp8 codec, some videos have N/A duration, where nb_frames, bit_rate are also broken (missing or 0)
    # for these videos, we can re-encode them to get the correct duration. ref: https://trac.ffmpeg.org/wiki/FFprobeTips
    duration = format.get('duration', 0.)
    if duration == 0:
        info_dict['missing_fields'].append('duration')
        warn_print(f'{video_path} has broken video duration. Container format is {format["format_name"]}, Codec is {video_stream["codec_name"]}')
        if force_decoding:
            _, err = get_cmd_output(f"ffmpeg -i {video_path} -f null -")
            err = err.replace('\r', '\n')
            # lines like: frame=  400 fps=0.0 q=-0.0 Lsize=N/A time=00:00:03.98 bitrate=N/A speed= 100x
            info_line = [x for x in err.split('\n') if x.startswith('frame=')][-1].strip()
            pattern = re.compile(r'(frame=)(.*)(fps.*time=)(.*)(bitrate.*)')
            res = pattern.findall(info_line)[0]

            duration_str = res[3].strip()
            hour, miniute, second = [float(x) for x in duration_str.split(':')]
            duration = hour * 3600 + miniute * 60 + second

            nb_frames = int(res[1].strip())
            fps = nb_frames / duration if duration >0 else 0.
        else:
            duration = -1
    else:
        duration = float(duration)
    
    original_height, original_width = int(video_stream['height']), int(video_stream['width'])

    tags = video_stream.get('tags')  # tags may be missing
    rotation = 0
    if tags:
        rotation = tags.get('rotate')
        if rotation:
            rotation = int(rotation)
        else:
            rotation = 0
    else:
        info_dict['missing_fields'].append('tags')

    if (rotation // 90) % 2 == 1:
        height, width = original_width, original_height
    else:
        height, width = original_height, original_width
    
    info_dict['codec_name'] = video_stream['codec_name']
    info_dict['duration'] = duration
    info_dict['nb_frames'] = nb_frames
    info_dict['fps'] = fps
    info_dict['height'] = height
    info_dict['width'] = width
    info_dict['rotation'] = rotation
    info_dict['original_height'] = original_height
    info_dict['original_width'] = original_width

    return info_dict


def ffmpeg_cut_video(src_video_path, dst_video_path, start_time, end_time=None, duration=None, accurate_cut=True):
    # start_time, end_time, duration must be seconds(float)
    assert os.path.exists(src_video_path), f'{src_video_path} does not exist'

    if end_time is None and duration is None:
        raise ValueError('Either end_time or duration must be specified')
    elif end_time is not None and duration is not None:
        raise ValueError('Only one of end_time or duration can be specified')
    
    if end_time is not None:
        assert end_time > start_time, f'end_time({end_time}) must be greater than start_time({start_time})'
        duration = end_time - start_time
    
    cut_cmd = f'ffmpeg -y -ss {Duration(start_time)} -i "{src_video_path}" -t {Duration(duration)} -c copy "{dst_video_path}"'
    if accurate_cut:
        cut_cmd = f'ffmpeg -y -ss {Duration(start_time)} -i "{src_video_path}" -t {Duration(duration)} "{dst_video_path}"'

    get_cmd_output(cut_cmd)


def rotate_video(video_path, angle, verbose=False):
    # rotate video by angle, angle is in degree and clockwise
    assert os.path.exists(video_path), f'{video_path} does not exist'
    assert isinstance(angle, int) and angle % 90 == 0, f'angle must be a multiple of 90, but got {angle}'

    angle = angle % 360

    # lossless rotate is achieved by modifying the metadata in place
    # ref: https://superuser.com/a/1307206/1010278, https://gist.github.com/hajoscher/2b77247ed714207ba59d6b13c1371000
    # but this only supports part mp4 container formats
    # fallback method is using ffmpeg modifying metadata, but it leads to lossy rotatation with re-encoding 
    # ref: https://ostechnix.com/how-to-rotate-videos-using-ffmpeg-from-commandline/
    # moreover, the fallback methods does not support generate avi and mkv outputs (change it to mp4 is ok)

    # inplace modify metadata hex code
    zero = bytes([0,   1,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
                  0,   1,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0, 
                  64])

    r90  = bytes([0,   0,   0,   0,   0,   1,   0,   0,   0,   0,   0,   0, 255, 255,   0,   0,
                  0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0, 
                  64])

    r180 = bytes([255, 255,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
                  255, 255,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
                  64])

    r270 = bytes([0,   0,   0,   0, 255, 255,   0,   0,   0,   0,   0,   0,   0,   1,   0,   0,
                  0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
                  64]) 
    
    angle_d = {0: zero, 90: r90, 180: r180, 270: r270}

    INPLACE_SUCCESS = False
    with open(video_path, "r+b") as f:
        mm = mmap.mmap(f.fileno(), 0)
        loc = mm.find(b'vide')
        if loc < 160:
            verbose_print(f'{video_path} does not support inplace modify metadata, first try failed (hex loc1 not found).', verbose)
        else:
            mm.seek(loc - 160)
            loc = mm.find(bytes([64])) - 32
            if loc < 0:
                verbose_print(f'{video_path} does not support inplace modify metadata, second try failed (hex loc2 not found).', verbose)
            else:
                mm.seek(loc)
                original_angle_hex = mm.read(33)
                if original_angle_hex in list(angle_d.values()):
                    verbose_print(f'{video_path} found original angle {list(angle_d.values()).index(original_angle_hex) * 90} in metadata, and will be changed to {angle} in place.', verbose)
                    mm.seek(loc)
                    mm.write(angle_d[angle])
                    INPLACE_SUCCESS = True
                else:
                    verbose_print(f'{video_path} does not support inplace modify metadata, third try failed (original angle not found).', verbose)
        mm.close()

    REPLACE_SUCCESS = False
    if not INPLACE_SUCCESS:
        verbose_print(f'{video_path} does not support inplace modify metadata, fallback to ffmpeg.', verbose)

        video_name = os.path.basename(video_path)
        video_prefix, video_suffix = os.path.splitext(video_name)

        if video_suffix.lower() in ['.avi', '.mkv']:
            new_video_path = video_path.replace(video_suffix, '.mp4')
            new_video_suffix = '.mp4'
            verbose_print(f'{video_path} with suffix {video_suffix} will be replaced with {new_video_suffix}.', verbose)
        elif video_suffix.lower() in ['.mp4', '.mov']:
            new_video_path = video_path
            new_video_suffix = video_suffix
        else:
            print(f"{video_path} with suffix {video_suffix} is not supported.")
            return INPLACE_SUCCESS, REPLACE_SUCCESS, video_path

        try:
            tmp_video_path = f'/tmp/{video_prefix}_{random.randint(0, 1e6)}{new_video_suffix}'
            # add minus symbol to use clockwise rotation
            cmd = f'ffmpeg -y -i "{video_path}" -c copy -metadata:s:v:0 rotate=-{angle} "{tmp_video_path}"'
            get_cmd_output(cmd)
            cmd = f'mv "{tmp_video_path}" "{new_video_path}"'
            get_cmd_output(cmd)
            REPLACE_SUCCESS = True
        except Exception as e:
            print(f'{video_path} rotate by ffmpeg failed. Err: {str(e)}')
    
        return INPLACE_SUCCESS, REPLACE_SUCCESS, new_video_path
    else:
        return INPLACE_SUCCESS, REPLACE_SUCCESS, video_path