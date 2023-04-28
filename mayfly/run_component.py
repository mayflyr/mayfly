
import asyncio

import json

from functools import partial

def aioruncompat_run(coro, **kwargs):
	
	stop_on_unhandled_errors = kwargs['stop_on_unhandled_errors']
	use_uvloop               = kwargs['use_uvloop']
	
	loop = asyncio.get_event_loop()
	result = loop.run_until_complete(coro)
	
	return

try:
	from aiorun import run
except:
	run = partial(aioruncompat_run, stop_on_unhandled_errors=True, use_uvloop=True)


import traceback
import uuid

from .core import _loop_run_components


import logging

logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)


async def _lifecycle_single_component(predef_uid, q_components, evt_stop, str_component_def, str_mayfly_repo, str_json_kwargs, str_requirements_jsonlst, str_loadimports):
	
	if predef_uid is not None:
		uid = predef_uid
	else:
		uid = f'{uuid.uuid4()}'
	
	t_kwargs = json.loads(str_json_kwargs)
	
	q_components.put_nowait([
		'LC_BEGIN', 
		{
			'uid': uid, 
			'def': [str_component_def, t_kwargs], 
			'str_requirements_jsonlst': str_requirements_jsonlst, 
			'str_loadimports': str_loadimports, 
			
			'str_mayfly_repo': str_mayfly_repo, 
		}
	])
	
	return


import socket
import os


from .utils import _read_stream, _convert_bytes_from_b64_spaced_ascii


async def _handle_socket_client_connected(func_lc_bus, reader, writer):
	
	logger.info(f'BEGIN2 _handle_socket_client_connected')
	
	lst_partials = []
	
	def func_callback(bmsg_full):
	
		logger.info(f'CALLBACK_B : {len(bmsg_full)}')
		
		try:
			
			bstr_pre = b''.join(lst_partials)
			lst_partials.clear()
			
			bmsg_fullest = bstr_pre + bmsg_full
			
			b_msgs, partial_out = _convert_bytes_from_b64_spaced_ascii(bmsg_fullest)
			
			if partial_out is not None:
				lst_partials.append(partial_out)
			
			msgs_decoded = [json.loads(x.decode('utf-8')) for x in b_msgs]
			
			for msg_def in msgs_decoded:
				
				func_lc_bus(msg_def)
		
		except:
			
			logger.error(traceback.format_exc())
		
		return
	
	await _read_stream(
		reader, 
		func_callback, 
	)
	
	logger.info(f'END2 _handle_socket_client_connected')
		
	return


async def _handle_lcbus_proxy_child_input(q_components_pull, q_components_push, t_uid):
	
	logger.info(f'BEGIN _handle_lcbus_proxy_child_input')
	
	f_name = f'/tmp/sock_{t_uid}_PUSH'
	
	try:
		os.remove(f_name)
	except OSError:
		pass
	
	def func_lc_bus(msg_def):
		
		q_components_pull.put_nowait(msg_def)
		
		return
	
	logger.info(f'CHILD starting unix server ... {f_name}')
	
	server = await asyncio.start_unix_server(partial(_handle_socket_client_connected, func_lc_bus), f_name)
	
	logger.info(f'CHILD waiting for unix server close ... {f_name}')
	
	async with server:
		await server.serve_forever()
	
	logger.info(f'CHILD unix server closed . {f_name}')
	
	logger.info(f'END _handle_lcbus_proxy_child_input')
	
	return


from .utils import _convert_bytes_to_b64_spaced_ascii


async def _handle_lcbus_proxy_child_output(q_components_pull, q_components_push, t_uid):
	
	logger.info(f'BEGIN _handle_lcbus_proxy_child_output')
	
	f_name = f'/tmp/sock_{t_uid}_PULL'
	
	max_retries = 20
	curr_retries = 0
	
	s = None
	
	while curr_retries < max_retries:
		try:
			s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
			s.connect(f_name)
		except:
			logger.info(f'child RETRYING socket : {f_name} ')
			curr_retries += 1
			await asyncio.sleep(1)
		else:
			break
	
	if s is None:
		raise Exception('COULD NOT GET SOCKET')
	
	try:
		
		while True:
			
			og_msg_def = await q_components_push.get()
			
			lst_msgdefs = [og_msg_def]
			while not q_components_push.empty():
				lst_msgdefs.append(q_components_push.get_nowait())
			
			
			for msg_def in lst_msgdefs:
				
				action_type, action_data = msg_def
				
				is_init_venvactor_run = False
				
				if is_init_venvactor_run:
					
					q_components_pull.put_nowait(msg_def)
				
				else:
					
					b_msg_preencode = json.dumps(msg_def).encode('utf-8')
					b_msg_out = _convert_bytes_to_b64_spaced_ascii(b_msg_preencode)
					
					s.send(b_msg_out)
					
					logger.info(f'SENT_B : {len(b_msg_out)}')
			
	finally:
		
		s.close()
	
	logger.info(f'END _handle_lcbus_proxy_child')
	
	return


async def _run_lcbus_proxy(lcbus_uid, q_components_pull, q_components_push):
	
	logger.info(f'BEGIN _run_lcbus_proxy')
	
	pending = set()
	
	fut_run_socket_in = asyncio.ensure_future(_handle_lcbus_proxy_child_input(q_components_pull, q_components_push, lcbus_uid))
	pending.add(fut_run_socket_in)
	
	fut_run_socket_out = asyncio.ensure_future(_handle_lcbus_proxy_child_output(q_components_pull, q_components_push, lcbus_uid))
	pending.add(fut_run_socket_out)
	
	while pending:
		
		success, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
		
		for fut_success in success:
			
			fut_result = fut_success.result()
	
	logger.info(f'END _run_lcbus_proxy')
	
	return


async def async_main(str_component_def, str_mayfly_repo, str_json_kwargs, str_loadimports, str_requirements_jsonlst, lcbus_uid, predef_uid):
	
	logger.info(f'before async_main')
	
	lst_coros = []
	
	evt_stop = asyncio.Event()
	
	if lcbus_uid is None:
		
		q_components_pull = asyncio.Queue()
		q_components_push = q_components_pull
		
		lst_coros += [
			_lifecycle_single_component(predef_uid, q_components_push, evt_stop, str_component_def, str_mayfly_repo, str_json_kwargs, str_requirements_jsonlst, str_loadimports), 
			_loop_run_components(q_components_pull, q_components_push, evt_stop, str_mayfly_repo)
		]
		
	else:
		
		q_components_pull = asyncio.Queue()
		q_components_push = asyncio.Queue()
		
		lst_coros += [
			_run_lcbus_proxy(lcbus_uid, q_components_pull, q_components_push), 
			_lifecycle_single_component(predef_uid, q_components_push, evt_stop, str_component_def, str_mayfly_repo, str_json_kwargs, str_requirements_jsonlst, str_loadimports), 
			_loop_run_components(q_components_pull, q_components_push, evt_stop, str_mayfly_repo)
		]
	
	await asyncio.gather(*lst_coros)
	
	logger.info(f'after async_main')
	logger.info(f'before stop loop')
	
	loop = asyncio.get_event_loop()
	loop.stop()
	
	logger.info(f'after stop loop')
	
	return


def main(*args, **kwargs):
	run(async_main(*args, **kwargs))
	return

