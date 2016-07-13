import datetime
import os

def log(text,log_file=None):
    print('[%s] %s' % (str(datetime.datetime.now().time())[:-7],text))
    if log_file:
        write_log(text)

def create_dir(path):
    new_dir = os.path.join(os.path.dirname(__file__),os.pardir,path)
    if not os.path.exists(new_dir):
        log('creating dir: %s' % new_dir)
        os.makedirs(new_dir)
    return new_dir
