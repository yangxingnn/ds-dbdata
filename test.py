# coding:utf-8

env = None
print 'asd'
if(env != None):
    print 'eu'


class Test(object):
    def __init__(name):
        self.name = name

    def print_name(name=name):
        print name

if __name__ == '__main__':
    test = Test('yang')
    test.print_name()
