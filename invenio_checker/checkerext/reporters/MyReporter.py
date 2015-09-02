class MyReporter(object):
    def __init__(self):
        pass

    def report(self, user_readable_msg, location_tuple):
        print user_readable_msg
        print '1'

    # def report_exception(self, when, outrep_summary, location_tuple, exc_info, formatted_exception):
    def report_exception(self, when, outrep_summary, location_tuple, formatted_exception=None, patches=None):
        print '~~~0'
        print when
        print '~~~1'
        print outrep_summary
        print '~~~2'
        print location_tuple
        print '~~~3'
            # print exc_info
            # print '~~~4'
        if formatted_exception is not None:
            print formatted_exception
            print '~~~5'
        if patches is not None:
            print patches
            print '~~~5'

def get_reporter(name=None):
    return MyReporter()
