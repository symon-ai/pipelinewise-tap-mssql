import traceback
import logging
import json

class SymonException(Exception):
    def __init__(self, message, code, details=None):
        super().__init__(message)
        self.code = code
        self.details = details

def raise_error(error_info, config):
    error_file_path = config.get('error_file_path', None)
    if error_file_path is not None:
        try:
            with open(error_file_path, 'w', encoding='utf-8') as fp:
                json.dump(error_info, fp)
        except:
            pass

    error_info_json = json.dumps(error_info)
    error_start_marker = config.get('error_start_marker', '[target_error_start]')
    error_end_marker = config.get('error_end_marker', '[target_error_end]')
    logger = logging.getLogger(__name__)
    logger.info(f'{error_start_marker}{error_info_json}{error_end_marker}')

    raise SymonException(error_info.get('message'), error_info.get('code'))