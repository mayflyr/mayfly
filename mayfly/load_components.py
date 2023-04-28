
path_user_root  = '/home/user'
path_pkg_mayfly = f'{path_user_root}/mount/local_pkgs/mayfly'
str_pycmd       = 'python3.11'


import asyncio

from uuid import uuid4
import json
import traceback
from functools import partial
import socket
import os
from base64 import b64encode
import ast
import types


from .utils import _read_stream
from .utils import generic_readavailable
from .utils import _convert_bytes_to_b64_spaced_ascii
from .utils import _convert_bytes_from_b64_spaced_ascii



import logging
logger = logging.getLogger(__name__)



async def _handle_socket_client_connected(func_lc_bus, reader, writer):
	
	logger.info(f'BEGIN _handle_socket_client_connected')
	
	lst_partials = []
	
	def func_callback(bmsg_full):
		
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
	
	logger.info(f'END _handle_socket_client_connected')
		
	return


def _oneoff_lcbus_proxy_parent_output(s, data):
	
	logger.info(f'BEGIN _oneoff_lcbus_proxy_parent_output')
	
	try:
		
		b_msg_preencode = json.dumps(data).encode('utf-8')
		b_msg_out = _convert_bytes_to_b64_spaced_ascii(b_msg_preencode)
		
		s.send(b_msg_out)
	
	except:
		
		logger.error(traceback.format_exc())
	
	logger.info(f'END _oneoff_lcbus_proxy_parent_output')
	
	return


async def _handle_lcbus_proxy_parent_input(t_uid, func_lc_bus):
	
	logger.info(f'BEGIN _handle_lcbus_proxy_parent_input')
	
	f_name = f'/tmp/sock_{t_uid}_PULL'
	
	try:
		os.remove(f_name)
	except OSError:
		pass
	
	logger.info(f'PARENT starting unix server ... {f_name}')
	
	server = await asyncio.start_unix_server(partial(_handle_socket_client_connected, func_lc_bus), f_name)
	
	logger.info(f'PARENT waiting for unix server close ... {f_name}')
	
	async with server:
		await server.serve_forever()
	
	logger.info(f'PARENT unix server closed . {f_name}')
	
	logger.info(f'END _handle_lcbus_proxy_parent_input')
	
	return


async def _handle_running_process(process):
	
	logger.info(f'BEGIN _handle_running_process')
	
	def stdout_cb(x):
		logger.info(f'STDOUT : {x}')
		return
	def stderr_cb(x):
		logger.info(f'STDERR : {x}')
		return
	
	await asyncio.gather(
		_read_stream(process.stdout, stdout_cb), 
		_read_stream(process.stderr, stderr_cb)
	)
	
	await process.wait()
	
	logger.info(f'END _handle_running_process')
	
	return


def _make_universal_component_cls(str_value, str_requirements_jsonlst, str_loadimports, str_mayfly_repo, func_lc_bus, q_msgs_inner, func_get_uid, funcs_msgfwd):
	
	if str_requirements_jsonlst is not None:
		out = _make_venv_component_cls(func_lc_bus, q_msgs_inner, funcs_msgfwd, str_value, str_requirements_jsonlst, str_loadimports, str_mayfly_repo)
	else:
		out = _make_aexec_component_cls(func_lc_bus, q_msgs_inner, func_get_uid, funcs_msgfwd, str_value, str_requirements_jsonlst, str_loadimports)
	
	return out


def _make_asyncfunc_component_cls(async_func):
	
	class AsyncfuncComponent:
		_parallel_funcs = [async_func]
	
	return AsyncfuncComponent


def _make_aexec_component_cls(func_lc_bus, q_msgs_inner, func_get_uid, funcs_msgfwd, f_str, str_requirements_jsonlst, str_loadimports):
	
	class AsyncExecProxyComponent:
		
		async def _parallel_async(self):
			
			d_globals = {
				'__builtins__': {
					'__build_class__': __build_class__, 
				}, 
				
				'__name__': __name__, 
				
				'asyncio': asyncio, 
				'range'  : range, 
				'json': json, 
				'traceback': traceback, 
				'set': set, 
				
				'uuid4': uuid4, 
				
				'dir': dir, 
				'str': str, 
				
				'b64encode': b64encode, 
				
				'logger': logger, 
				
				'LC_BUS': func_lc_bus, 
				'UID': func_get_uid(), 
				
				'Q_MSGS_INNER': q_msgs_inner, 
				
				'INIT_KWARGS': self._init_kwargs, 
			}
			
			if str_loadimports is not None:
				
				imp_globals = {
					'__builtins__': {
						'__import__': __import__, 
					},
				}
				
				imp_locals = {}
				
				exec(str_loadimports, imp_globals, imp_locals)
				
				d_globals.update(imp_locals)
			
			code_obj = compile(f_str, '<string>', 'exec', flags=ast.PyCF_ALLOW_TOP_LEVEL_AWAIT)
			
			f = types.FunctionType(code_obj, d_globals)
			
			await f()
			
			return
		
		_parallel_funcs = [
			_parallel_async, 
		]
		
		def __init__(self, init_kwargs):
			
			self._init_kwargs = init_kwargs
			
			return
	
	return AsyncExecProxyComponent


def _make_venv_component_cls(func_lc_bus, q_msgs_inner, funcs_msgfwd, f_str, str_requirements_jsonlst, str_loadimports, str_mayfly_repo):
	
	func_registermsgfwd, func_unregistermsgfwd = funcs_msgfwd
	
	predef_uid = f'{uuid4()}'
	
	internal_uid_for_venv = f'{uuid4()}'.replace('-','')
	
	class VenvProxyComponent:
		
		async def _handle_events(self):
			
			while True:
				
				full_data = await q_msgs_inner.get()
				
				logger.info(f'HANDLE EVENTS : {str(full_data)[:100]}')
				
				action_type, data = full_data
				
				if action_type == 'handle_intercepted':
					
					await self._do_handle_intercept(data)
			
			return
		
		async def _setup_socket_out(self):
			
			f_name = f'/tmp/sock_{internal_uid_for_venv}_PUSH'
			
			max_retries = 20
			curr_retries = 0
			
			s = None
			
			while curr_retries < max_retries:
				try:
					s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
					s.connect(f_name)
				except:
					logger.info(f'RETRYING socket')
					curr_retries += 1
					await asyncio.sleep(1)
				else:
					break
			
			if s is None:
				raise Exception('COULD NOT GET SOCKET')
			
			self._socket_out = s
			self._waitforsocket.set()
			
			return
			
		def _teardown_socket_out(self):
			
			if self._socket_out is not None:
				
				self._socket_out.close()
				self._waitforsocket.clear()
				
				self._socket_out = 'CLOSED'
			
			else:
				
				logger.warning(f'no socket exists, skipping teardown')
			
			return
		
		async def _do_handle_intercept(self, data):
			
			logger.info(f'BEGIN _do_handle_intercept : {str(data)[:100]}')
			
			if self._socket_out != 'CLOSED':
				
				if not self._waitforsocket.is_set():
					await self._waitforsocket.wait()
				
				_oneoff_lcbus_proxy_parent_output(self._socket_out, data)
				
			else:
				
				logger.error(f'UNEXPECTED : self._socket_out = CLOSED')
			
			logger.info(f'END _do_handle_intercept')
			
			return
		
		async def _parallel_async(self):
			
			self._do_startup()
			
			logger.info(f'VenvProxyComponent BEGIN _parallel_async')
			
			try:
				
				full_pkg_str = ' '.join(json.loads(str_requirements_jsonlst))
				
				if str_mayfly_repo is None:
					raise Exception(f'unexpected: str_mayfly_repo is NULL')
				
				path_pkg_mayfly = str_mayfly_repo
				
				cmd1 = [
					'/bin/bash', 
					'-c', 
					f'cd {path_user_root} && {str_pycmd} -m venv venv_{internal_uid_for_venv} && . ./venv_{internal_uid_for_venv}/bin/activate && pip install --no-cache-dir --upgrade pip && pip install --no-cache-dir {full_pkg_str} {path_pkg_mayfly}'
				]
				
				process1 = await asyncio.create_subprocess_exec(
					*cmd1,
				)
				
				await process1.communicate()
				
				str_json_kwargs = json.dumps(self._init_kwargs)
				
				alt_requirements_jsonlst = ''
				
				cmd2 = [
					f'{path_user_root}/venv_{internal_uid_for_venv}/bin/python', '-m', 'mayfly', 
					
					f_str, str_mayfly_repo, str_json_kwargs, str_loadimports,
					
					alt_requirements_jsonlst,
					
					internal_uid_for_venv, predef_uid
				]
				
				process2 = await asyncio.create_subprocess_exec(
					*cmd2, 
					
					stdout=asyncio.subprocess.PIPE, 
					stderr=asyncio.subprocess.PIPE, 
				)
				
				await asyncio.gather(
					self._setup_socket_out(), 
					
					_handle_lcbus_proxy_parent_input(internal_uid_for_venv, func_lc_bus), 
					
					_handle_running_process(process2), 
				)
			
			except:
				logger.error(traceback.format_exc())
			
			logger.info(f'VenvProxyComponent END _parallel_async')
			
			self._do_shutdown()
			self._teardown_socket_out()
			
			return
		
		def _do_startup(self):
			
			func_registermsgfwd(predef_uid, 'handle_intercepted')
			
			return
		
		def _do_shutdown(self):
			
			func_unregistermsgfwd(predef_uid, 'handle_intercepted')
			
			return
		
		_parallel_funcs = [
			_handle_events, 
			_parallel_async, 
		]
		
		def __init__(self, init_kwargs):
			
			self._init_kwargs = init_kwargs
			
			logger.info(f'DEBUG init_kwargs : {init_kwargs}')
			
			self._q_output = asyncio.Queue()
			
			self._socket_out = None
			self._waitforsocket = asyncio.Event()
			
			logger.info(f'VenvProxyComponent : init_kwargs={init_kwargs} | f_str={f_str[:100]} | str_requirements_jsonlst={str_requirements_jsonlst} | str_loadimports={str_loadimports}')
			
			return
	
	return VenvProxyComponent

