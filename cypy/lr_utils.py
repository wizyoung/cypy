import torch
from torch.optim.lr_scheduler import _LRScheduler


class GradualWarmupScheduler(_LRScheduler):
    """ Gradually warm-up(increasing) learning rate in optimizer.
    Proposed in 'Accurate, Large Minibatch SGD: Training ImageNet in 1 Hour'.
    Args:
        optimizer (Optimizer): Wrapped optimizer.
        multiplier: target learning rate = base lr * multiplier if multiplier > 1.0. if multiplier = 1.0, lr starts from 0 and ends up with the base_lr.
        total_epoch: target learning rate is reached at total_epoch, gradually
        after_scheduler: after target_epoch, use this scheduler(eg. ReduceLROnPlateau)

    NOTE: referred from "https://github.com/ildoonet/pytorch-gradual-warmup-lr" with modifications
    modification1: in `get_lr` func, change `self.last_epoch` to `self.last_epoch + 1`, so that we can normally call schehuder.step() after optimizer.step()
    """
    
    def __init__(self, optimizer, multiplier, total_epoch, after_scheduler=None):
        self.multiplier = multiplier
        if self.multiplier < 1.:
            raise ValueError('multiplier should be greater than or equal to 1.')
        self.total_epoch = total_epoch
        self.after_scheduler = after_scheduler
        self.finished = False
        super(GradualWarmupScheduler, self).__init__(optimizer)
        
    def get_lr(self):
        if self.last_epoch >= self.total_epoch:
            if self.after_scheduler:
                if not self.finished:
                    self.after_scheduler.base_lrs = [base_lr * self.multiplier for base_lr in self.base_lrs]
                    self.finished = True
                return self.after_scheduler.get_last_lr()
            return [base_lr * self.multiplier for base_lr in self.base_lrs]
        
        if self.multiplier == 1.0:
            return [base_lr * (float(self.last_epoch + 1) / self.total_epoch) for base_lr in self.base_lrs]
        else:
            return [base_lr * ((self.multiplier - 1.) * (self.last_epoch + 1) / self.total_epoch + 1.) for base_lr in self.base_lrs]
                
    def step(self, epoch=None, metrics=None):
        if self.finished and self.after_scheduler:
            if epoch is None:
                self.after_scheduler.step(None)
            else:
                self.after_scheduler.step(epoch - self.total_epoch)
            self.last_epoch = self.after_scheduler.last_epoch + self.total_epoch + 1
            self._last_lr = self.after_scheduler.get_last_lr()
        else:
            return super(GradualWarmupScheduler, self).step(epoch)

if __name__ == "__main__":

    from torch.optim.lr_scheduler import MultiStepLR, StepLR
    import numpy as np
    import matplotlib.pyplot as plt
    
    t = torch.tensor([0.0], requires_grad=True)
    optim = torch.optim.SGD([t], lr=0.01)

    lr_scheduler = MultiStepLR(optim, milestones=[5, 8])
    #lr_scheduler = StepLR(optim, step_size=1, gamma=0.1)
    scheduler_warmup = GradualWarmupScheduler(optim, multiplier=1, total_epoch=4, after_scheduler=lr_scheduler)

    lrs = []
    for e in range(1, 20):
        print(e, optim.param_groups[0]['lr'], scheduler_warmup.last_epoch)
        optim.step()
        scheduler_warmup.step()
        lrs.append((e, optim.param_groups[0]['lr']))

    #lrs = np.array(lrs)
    #print(lrs)
    #plt.plot(lrs[:, 0], lrs[:, 1])
    #plt.show()