import os
import pprint
import random
import string
import cv2
from decord import VideoReader
from tqdm import tqdm
from collections import OrderedDict

from cypy.misc_utils import get_cmd_output
from cypy.logging_utils import getLogger
from cypy.cli_utils import warn_print
from cypy.time_utils import Duration

import ffmpeg
import re


# TODO: use ffmpeg to convert and detect is better in the future
def detect_broken_duration_video(inp, format='file', strict_check=False, convert=False, convert_params=None, progress=True, verbose=False):
    assert format in ['file', 'list', 'txt']

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
    
    output_normal = []
    output_abnormal = []
    logger = getLogger(random_route, console=True)

    if progress:
        all_video_paths = tqdm(all_video_paths)

    for idx, video_path in enumerate(all_video_paths):
        assert os.path.exists(video_path), f'The {idx}th item [{video_path}] does not exist'
        try:
            vr = VideoReader(video_path)
            if strict_check:
                img = vr[0].asnumpy()
                h, w, c = img.shape
                img = cv2.resize(img, (w//2, h//2))
            output_normal.append(video_path)
        except:
            if verbose:
                logger.warning(f'VIDEO DETECT ERR: {video_path}')
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
                if verbose:
                    print(stderr)
                # print(cmd1)

                if os.path.getsize(tmp_vid_file_name) == 0:
                    logger.error(f'FFMPEG REENCODE ERR: {video_path}')
                    get_cmd_output(f'rm -rf {tmp_vid_file_name}')
                else:
                    cmd2 = f'mv {tmp_vid_file_name} "{video_path}"'
                    get_cmd_output(cmd2)
                    # print(cmd2)

    return output_normal, output_abnormal


def get_video_info(video_path, force_decoding=False):
    assert os.path.exists(video_path), f'{video_path} does not exist'

    # contains: duration(float), nb_frames(int), fps(float), height(int), width(int), rotation(int), original_height(int), original_width(int), codec_name(str)
    # -1 value means unknown or broken or missing
    info_dict = {}

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

    tags = video_stream['tags']
    rotation = tags.get('rotate')
    if rotation:
        rotation = int(rotation)
    else:
        rotation = 0

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

