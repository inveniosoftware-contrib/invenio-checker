import mock

class MyReporter(object):
    def report(self, user_readable_msg, location_tuple):
        print user_readable_msg
        print '1'

    def report_exception(self, outrep_summary, location_tuple, exc_info, formatted_exception):
        print outrep_summary
        print '~~~1'
        print formatted_exception
        print '~~~2'

def get_by_name(foo):
    return MyReporter()
