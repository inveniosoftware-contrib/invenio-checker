"""Collection of tasks used for reporting with Holdingpen and Workflows modules."""

def report_error(obj, eng):
    """Function to register error in Holdingpen"""
    eng.halt(msg='Checker rule: {0} reported an error'.format(obj.data['rule_name']))
