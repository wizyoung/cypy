

class AverageMeter(object):
    """Computes and stores the average and current value.
 
        Code imported from https://github.com/pytorch/examples/blob/master/imagenet/main.py#L247-L262
    """
 
    def __init__(self, name='metric', fmt=':f', sep=": "):
        self.name = name
        self.fmt = fmt
        self.sep = sep
        self.reset()
 
    def reset(self):
        self.val = 0
        self.avg = 0
        self.sum = 0
        self.count = 0
 
    def update(self, val, n=1):
        self.val = val
        self.sum += val * n
        self.count += n
        if self.count == 0:
           self.avg = self.sum / (self.count + 1e-12)
        else:
           self.avg = self.sum / self.count
 
    def __str__(self):
        fmtstr = '{name}{sep}{val' + self.fmt + '} ({avg' + self.fmt + '})'
        return fmtstr.format(**self.__dict__)
   

class ProgressMeter(object):
    def __init__(self, num_batches, meters, prefix=""):
       self.batch_fmtstr = self._get_batch_fmtstr(num_batches)
       self.meters = meters
       self.prefix = prefix
      
    def display(self, batch):
       entries = [self.prefix + self.batch_fmtstr.format(batch)]
       entries += [str(meter) for meter in self.meters]
       return '  '.join(entries)
      
    def _get_batch_fmtstr(self, num_batches):
       num_digits = len(str(num_batches // 1))
       fmt = '{:' + str(num_digits) + 'd}'
       return '[' + fmt + '/' + fmt.format(num_batches) + ']'


if __name__ == "__main__":
    meter = AverageMeter('TrainACC', ":.2f", ": ")
    pm = ProgressMeter(100, [meter], prefix="Test: ")
    meter.update(1)
    print(pm.display(1))
    meter.update(2)
    print(pm.display(2))
    meter.update(1)

   # print(meter)
