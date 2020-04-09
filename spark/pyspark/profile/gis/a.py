class _OneColDecorator(object):
    def __init__(self, f, line):
        self._function_name = f.__name__
        self._line = line

    def __call__(self):
        print(self._line)

def OneColDecorator(f=None, line=""):
    if f:
        return _OneColDecorator(f)
    else:
        def wrapper(f):
            return _OneColDecorator(f,line)
        return wrapper


@OneColDecorator(line = "LINESTRINg(dafd)")
def gen_st_geometry_type():
    pass

if __name__ == "__main__":
    gen_st_geometry_type()