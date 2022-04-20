import argparse
import torch
import torch.nn as nn
import torch.nn.functional as F
import torch.optim as optim
import torch.utils.data
import time

from cypy.cli_utils import simple_cli

class OccupyGPUDummyDataset(torch.utils.data.Dataset):
    def __init__(self, data_len=1000):
        self.data_len = data_len

    def __len__(self):
        return self.data_len

    def __getitem__(self, idx):
        return torch.randn(1, 28, 28), torch.randint(0, 10, (1,))[0]

class Net(nn.Module):
    def __init__(self):
        super(Net, self).__init__()
        self.conv1 = nn.Conv2d(1, 128, 3, 1)
        self.conv2 = nn.Conv2d(128, 512, 3, 1)
        self.conv4 = nn.Conv2d(512, 1024, 3, 1)
        self.conv5 = nn.Conv2d(1024, 256, 3, 1)
        self.dropout1 = nn.Dropout2d(0.25)
        self.dropout2 = nn.Dropout2d(0.5)
        self.fc1 = nn.Linear(6400*4, 64)
        self.fc2 = nn.Linear(64, 10)

    def forward(self, x):
        x = self.conv1(x)
        x = F.relu(x)
        x = self.conv2(x)
        x = self.conv4(x)
        x = self.conv5(x)
        x = F.relu(x)
        x = F.max_pool2d(x, 2)
        x = self.dropout1(x)
        x = torch.flatten(x, 1)
        # print(x.shape)
        x = self.fc1(x)
        x = F.relu(x)
        x = self.dropout2(x)
        x = self.fc2(x)
        output = F.log_softmax(x, dim=1)
        return output


def train(model, train_loader, optimizer, sleep_time):
    model.train()
    for batch_idx, (data, target) in enumerate(train_loader):
        data, target = data.cuda(), target.cuda()
        optimizer.zero_grad()
        output = model(data)
        loss = F.nll_loss(output, target)
        loss.backward()
        optimizer.step()   
        time.sleep(sleep_time)         


def main():

    args = simple_cli(
        batch_size=8,
        epochs=int(1e10),
        lr=0.1,
        gpu_level=1.0
    )

    assert torch.cuda.is_available(), "CUDA is not available!"
    
    train_loader = torch.utils.data.DataLoader(
        OccupyGPUDummyDataset(data_len=args.batch_size * 500),
        batch_size=args.batch_size, shuffle=False)

    model = Net()
    model = torch.nn.DataParallel(model).cuda()
    optimizer = optim.Adadelta(model.parameters(), lr=args.lr)

    epoch_time = 0
    sleep_time = 0
    for epoch in range(1, args.epochs):
        print('>>> Entering epoch {}'.format(epoch))
        if epoch == 2:
            start = time.time()
            train(model, train_loader, optimizer, sleep_time)
            epoch_time = time.time() - start
            sleep_time = epoch_time / 500 * (1 / args.gpu_level - 1)
            print(f"EPOCH {epoch} cost {epoch_time} s")
        else:
            train(model, train_loader, optimizer, sleep_time)

if __name__ == '__main__':
    main()
