import logging
import os
import json
import requests

logger = logging.getLogger(__name__)

HDX_URL = os.getenv('HDX_URL')

HDX_PCODE_PATCH_URL = f'{HDX_URL}/api/action/hdx_p_coded_resource_update'

HEADERS = {
    'Content-type': 'application/json',
    'Authorization': os.getenv('HDX_KEY')
}

def patch_resource_with_pcode_value(resource_id: str, pcode_value: bool) -> None:
    if pcode_value is not None:
        body = {
            'id': resource_id,
            'p_coded': pcode_value
        }
        r = requests.post(HDX_PCODE_PATCH_URL, data=json.dumps(body), headers=HEADERS)
        r.raise_for_status()
    else:
        logger.warning(f'Did not update resource {resource_id} because calculated value would be None')


