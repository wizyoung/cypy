import os
import random
import string
import cv2
from decord import VideoReader
from tqdm import tqdm
from collections import OrderedDict

from cypy.misc_utils import get_cmd_output
from cypy.logging_utils import getLogger

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
        for key in convert_params:
            assert key in ['global_params', 'input_params', 'output_params']

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
