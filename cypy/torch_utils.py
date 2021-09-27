import torch
import torch.distributed as dist

def reduce_tensor(tensor, mean=True):
    rt = tensor.clone()  # The function operates in-place.
    dist.all_reduce(rt, op=dist.ReduceOp.SUM)
    if mean:
        rt /= dist.get_world_size()
    return rt


def gather_tensor(inp, world_size, dist_=False, to_numpy=False):
    inp = torch.stack(inp)
    if dist_:
        gather_inp = [torch.ones_like(inp) for _ in range(world_size)]
        dist.all_gather(gather_inp, inp)
        gather_inp = torch.cat(gather_inp)
    else:
        gather_inp = inp
    
    if to_numpy:
        gather_inp = gather_inp.cpu().numpy()
    
    return gather_inp


class Compose:
    """
    all kwargs for __call__ func.
    """

    def __init__(self, transforms_):
        self.transforms_ = transforms_

    def __call__(self, img, **kwargs):
        for tt in self.transforms_:
            try:
                img = tt(img, **kwargs)
            except:
                print(f'ERROR in torch.utils.Compose (ComposeV2): {tt.__class__.__name__}')
                raise
        return img

    def __repr__(self):
        format_string = self.__class__.__name__ + '('
        for t in self.transforms_:
            format_string += '\n'
            format_string += '    {0}'.format(t)
        format_string += '\n)'
        return format_string
