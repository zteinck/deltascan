from functools import wraps
import time

from clockwork import format_duration


def print_status(func):

    @wraps(func)
    def wrapper(self, *args, **kwargs):

        if self.verbose:
            start_time = time.time()
            line = ' ' * 4 + f'• Scann'
            items = ' '.join(func.__name__.split('_')[2:])
            print(f'{line}ing {items}...', end='\r')

        out = func(self, *args, **kwargs)

        if self.verbose:
            duration = format_duration(time.time() - start_time)
            print(f'{line}ed {items} in {duration}.\033[K')

        return out

    return wrapper