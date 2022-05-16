import numpy as np
import torch
import torch.utils.data

from transformer import Constants


class EventData(torch.utils.data.Dataset):
    """ Event stream dataset. """

    def __init__(self, data):
        """
        Data should be a list of event streams; each event stream is a list of dictionaries;
        each dictionary contains: time_since_start, time_since_last_event, type_event
        """
        self.long = [[elem['long'] for elem in inst] for inst in data]
        self.short = [[elem['short'] for elem in inst] for inst in data]
        # plus 1 since there could be event type 0, but we use 0 as padding
        self.event_type = [[elem['type_event'] for elem in inst] for inst in data]

        self.length = len(data)

    def __len__(self):
        return self.length

    def __getitem__(self, idx):
        """ Each returned element is a list, which represents an event stream """
        return self.long[idx], self.short[idx], self.event_type[idx]


def pad_time(insts):
    """ Pad the instance to the max seq length in batch. """

    max_len = max(len(inst) for inst in insts)

    batch_seq = np.array([
        inst + [Constants.PAD] * (max_len - len(inst))
        for inst in insts])

    return torch.FloatTensor(batch_seq)


def pad_type(insts):
    """ Pad the instance to the max seq length in batch. """

    max_len = max(len(inst) for inst in insts)

    batch_seq = np.array([
        inst + [Constants.PAD] * (max_len - len(inst))
        for inst in insts])

    return torch.FloatTensor(batch_seq)


def collate_fn(insts):
    """ Collate function, as required by PyTorch. """
    '''
    temp_lst = []
    for x in insts[0]:
        if len(x) >= 7:
            try:
                temp_lst.append(x[-7:])
            except:
                pass

    insts = [tuple(temp_lst)]
    print('SIZES_1')
    for x in insts[0]:
        print(len(x))

    '''

    long, short, event_type = list(zip(*insts))
    return long, short, event_type


def get_dataloader(data, batch_size, shuffle=False):
    """ Prepare dataloader. """

    #ds = EventData(data)
    dl = torch.utils.data.DataLoader(
        data,
        num_workers=0,
        batch_size=batch_size,
        shuffle=shuffle
    )
    return dl