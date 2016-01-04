from collections import Counter

class CounterExt(Counter):
    def __init__(self):
        super(CounterExt, self).__init__(*args, **kwargs)
        self.total=0

    
