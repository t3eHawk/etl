import pypyrus_logbook as logbook
import pypyrus_logbook.logger as logger

def make_log(obj=None):
    if isinstance(obj, logger.Logger) is True:
        log = obj
    else:
        log = logbook.Logger()
    return log

log_columns = [
    {
        'name': 'oper_id',
        'type': 'integer',
        'sequence': True,
        'primary_key': True
    },
    {
        'name': 'oper_timestamp',
        'type': 'date'
    },
    {
        'name': 'initiator',
        'type': 'varchar',
        'length': 80
    },
    {
        'name': 'job',
        'type': 'integer'
    },
    {
        'name': 'start_timestamp',
        'type': 'date'
    },
    {
        'name': 'end_timestamp',
        'type': 'date'
    },
    {
        'name': 'input_count',
        'type': 'integer'
    },
    {
        'name': 'output_count',
        'type': 'integer'
    },
    {
        'name': 'update_count',
        'type': 'integer'
    },
    {
        'name': 'error_count',
        'type': 'integer'
    },
    {
        'name': 'status',
        'type': 'varchar',
        'length': 1
    }
]
