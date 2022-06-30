import random
import torch
import warnings
import numpy as np


def set_seed(SEED):
    if SEED is not None:
        random.seed(SEED)
        np.random.seed(SEED)
        torch.manual_seed(SEED)
        torch.cuda.manual_seed(SEED)
        torch.cuda.manual_seed_all(SEED)
        torch.backends.cudnn.deterministic = True
 
 
def setup(args):
    warnings.filterwarnings("ignore")
    set_seed(args.seed)