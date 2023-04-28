
import asyncio

from base64 import b64encode, b64decode

import logging
logger = logging.getLogger(__name__)

async def generic_readavailable(_self):
	
	n = 1
	
	if n < 0:
		raise ValueError('readexactly size can not be less than zero')
	
	if _self._exception is not None:
		raise _self._exception
	
	if n == 0:
		return b''
	
	if _self._eof:
		incomplete = bytes(_self._buffer)
		_self._buffer.clear()
		raise asyncio.IncompleteReadError(incomplete, n)
	
	await _self._wait_for_data('generic_readavailable')
	
	data = bytes(_self._buffer)
	_self._buffer.clear()
	
	_self._maybe_resume_transport()
	
	return data


async def _read_stream(stream, cb, until=None):
	
	logger.info(f'BEGIN _read_stream : {stream}')
	
	while True:
		try:	
			if until is None:
				line = await generic_readavailable(stream)
			else:
				line = await stream.readuntil(until)
		except asyncio.exceptions.IncompleteReadError:
			break
		except:
			logger.error(traceback.format_exc())
			break
		else:
			cb(line)
	
	logger.info(f'END _read_stream : {stream}')
	
	return


def _convert_bytes_to_b64_spaced_ascii(bytes_in):
	
	bytes_out = b64encode(bytes_in) + b' '
	
	return bytes_out


def _convert_bytes_from_b64_spaced_ascii(bytes_in):
	
	lst_split = bytes_in.split(b' ')	
	
	if lst_split[-1] != b'':
		b_msgs = [b64decode(x) for x in lst_split[:-1]]
		partial_out = lst_split[-1]
	else:
		b_msgs = [b64decode(x) for x in lst_split[:-1]]
		partial_out = None
	
	return b_msgs, partial_out

