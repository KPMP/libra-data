import threading

class DLUFileMoveThreaded(threading.Thread):
    def __init__(self):
        super(DLUFileMoveThreaded, self).__init__()

    def run(self):
        # run some code here
        print('Threaded task has been completed')