from flask import current_app
import os

def get_eliot_log_path():
    app = current_app

    eliot_log_path = os.path.join(
        app.instance_path,
        app.config.get('CFG_LOGDIR', ''),
        'checker',
    )

    if not os.path.exists(eliot_log_path):
        os.mkdir(eliot_log_path)

    return eliot_log_path
