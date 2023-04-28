import asyncio

import inspect
import uuid
from functools import partial

import logging
logger = logging.getLogger(__name__)

from .load_components import _make_universal_component_cls


def _callfunc_or_makecls_with_optional_args(obj, args_for_obj):
	
	is_cls_constructor = inspect.isfunction(obj) is False
	
	spec = inspect.getfullargspec(obj)
	num_args_to_obj = len(spec.args)
	
	result = None
	
	if is_cls_constructor:
		
		if num_args_to_obj == 0 or num_args_to_obj == 1:
			
			logger.warning(f'args provided but will not be consumed : {args_for_obj}')
			
			result = obj()
		
		else:
			
			result = obj(*args_for_obj)
	
	else:
		
		if num_args_to_obj == 0:
			
			logger.warning(f'args provided but will not be consumed : {args_for_obj}')
			
			result = obj()
			
		else:
			
			result = obj(*args_for_obj)
	
	return result


async def _wrap_handle_event(evt_type, coro):
	
	logger.info(f'begin ASYNC event handler... : {evt_type}')
	
	result = await coro
	
	logger.info(f'end ASYNC event handler : {evt_type}')
	
	return result

async def _wrap_func_startup(idx, coro):
	
	logger.info(f'begin ASYNC startup func... : {idx}')
	
	result = await coro
	
	logger.info(f'end ASYNC startup func... : {idx}')
	
	return result

async def _wrap_func_parallel(idx, coro):
	
	logger.info(f'begin ASYNC parallel func... : {idx}')
	
	result = await coro
	
	logger.info(f'end ASYNC parallel func... : {idx}')
	
	return result

async def _wrap_func_shutdown(idx, coro):
	
	logger.info(f'begin ASYNC shutdown func... : {idx}')
	
	result = await coro
	
	logger.info(f'end ASYNC shutdown func... : {idx}')
	
	return result

async def _wrap_wait_shutdown(uid, component):
	
	logger.info(f'waiting for shutdown... : {uid}')
	
	await component._shutdown_complete.wait()
	
	logger.info(f'shutdown complete. : {uid}')
	
	return


class LiveComponent:
	
	def __init__(self, q_components_push, init_params, funcs_msgfwd, active_mayflyrepo):
		
		self._uid = init_params['uid']
		
		self._q_components_push = q_components_push
		
		self._init_params = init_params
		
		def_type, def_kwargs = self._init_params['def']
		
		try:
			str_requirements_jsonlst = self._init_params['str_requirements_jsonlst']
		except KeyError:
			str_requirements_jsonlst = None
		try:
			str_loadimports = self._init_params['str_loadimports']
		except KeyError:
			str_loadimports = None
		try:
			str_mayfly_repo = self._init_params['str_mayfly_repo']
			logger.info(f'RUNTIME {str_mayfly_repo=}')
		except KeyError:
			str_mayfly_repo = active_mayflyrepo
			logger.info(f'PROCESS {str_mayfly_repo=}')
		
		self._q_msgs_inner = asyncio.Queue()
		
		cls = _make_universal_component_cls(def_type, str_requirements_jsonlst, str_loadimports, str_mayfly_repo, self._lc_bus, self._q_msgs_inner, self._get_uid, funcs_msgfwd)
		
		core_obj = _callfunc_or_makecls_with_optional_args(cls, [def_kwargs])
		
		self._core = core_obj
		
		self._evt_stop = asyncio.Event()
		
		self._evts_in = asyncio.Queue()
		
		self._shutdown_complete = asyncio.Event()
		
		return
	
	def _get_uid(self):
		return f'{self._uid}'
	
	def _lc_bus(self, msg):
		
		self._q_components_push.put_nowait(msg)
		
		return
	
	def deliver_message(self, data):
		
		self._evts_in.put_nowait(['MSG', data])
		
		return
	
	async def run(self):
		
		logger.info(f'LiveComponent run() BEGIN : {self._uid}')
		
		set_parallel_futs = set()
		
		funcs_parallel = getattr(self._core, '_parallel_funcs', [])
		
		for idx, func_parallel in enumerate(funcs_parallel):
			
			if inspect.iscoroutinefunction(func_parallel):
				
				t_coro = _callfunc_or_makecls_with_optional_args(func_parallel, [self._core])
				
				fut_parallel = asyncio.ensure_future(_wrap_func_parallel(idx, t_coro))
				set_parallel_futs.add(fut_parallel)
			
			else:
				raise Exception('sync func UNSUPPORTED here')
		
		pending = set()
		
		pending.update(set_parallel_futs)
		
		fut_stop = asyncio.ensure_future(self._evt_stop.wait())
		pending.add(fut_stop)
		
		fut_evt_in = asyncio.ensure_future(self._evts_in.get())
		pending.add(fut_evt_in)
		
		set_events_handling = set()
		
		while pending:
			
			success, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
			
			do_cancel = False
			
			for fut_success in success:
				
				fut_result = fut_success.result()
				
				if fut_success is fut_stop:
					
					logger.info(f'LiveComponent DOING CANCEL : {self._uid}')
					
					do_cancel = True
				
				elif fut_success is fut_evt_in:
					
					lst_evts = [fut_result]
					while not self._evts_in.empty():
						lst_evts.append(self._evts_in.get_nowait())
					
					fut_evt_in = asyncio.ensure_future(self._evts_in.get())
					pending.add(fut_evt_in)
					
					for evt_def in lst_evts:
						
						logger.info(f'LiveComponent EVENT IN : {str(evt_def)[:100]}')
						
						evt_type, evt_data = evt_def
						
						if evt_type == 'MSG':
							
							subevt_type, subevt_data = evt_data
							
							self._q_msgs_inner.put_nowait((subevt_type, subevt_data))
							
						else:
							logger.error(f'unexpected evt_type : {evt_type}')
				
				elif fut_success in set_events_handling:
					
					set_events_handling.remove(fut_success)
				
				elif fut_success in set_parallel_futs:
					
					set_parallel_futs.remove(fut_success)
					
					logger.info(f'async parallel fut complete')
			
			if do_cancel:
				
				for fut in pending:
					fut.cancel()
					try:
						await fut
					except asyncio.CancelledError:
						pass
				
				pending.clear()
		
		self._shutdown_complete.set()
		
		logger.info(f'LiveComponent run() END : {self._uid}')
		
		return self._uid
	
	def stop(self, reason):
		
		logger.info(f'LiveComponent STOP signal : {self._uid} : {reason}')
		
		if self._evt_stop.is_set():
			logger.info(f'(already shutting down, ignoring)')
		else:
			self._evt_stop.set()
		
		return


def register_msgfwd(d_msgfwd, deliver_to_uid, intercept_uid, handler_name):
	
	logger.info(f'REGISTER msgfwd : {intercept_uid} | {deliver_to_uid} | {handler_name}')
	
	t_key = (deliver_to_uid, handler_name)
	
	try:
		d_intercepts = d_msgfwd[intercept_uid]
	except KeyError:
		d_intercepts = {}
		d_msgfwd[intercept_uid] = d_intercepts
	
	if t_key in d_intercepts:
		logger.error('UNEXPECTED : t_key already registered : {intercept_uid} | {t_key}')
	else:
		d_intercepts[t_key] = True
	
	return


def unregister_msgfwd(d_msgfwd, deliver_to_uid, intercept_uid, handler_name):
	
	logger.info(f'UNREGISTER msgfwd : {intercept_uid} | {deliver_to_uid} | {handler_name}')
	
	t_key = (deliver_to_uid, handler_name)
	
	try:
		d_intercepts = d_msgfwd[intercept_uid]
	except KeyError:
		logger.error('UNEXPECTED : intercept_uid not in d_msgfwd : {intercept_uid}')
	else:
		try:
			del d_intercepts[t_key]
		except KeyError:
			logger.error('UNEXPECTED : t_key not in d_intercepts : {t_key}')
	
	return


async def _loop_run_components(q_components_pull, q_components_push, evt_stop, active_mayflyrepo):
	
	logger.info(f'_loop_run_components BEGIN')
	
	d_components = {}
	
	d_msgfwd = {}
	
	pending = set()
	
	fut_new_q = asyncio.ensure_future(q_components_pull.get())
	pending.add(fut_new_q)
	
	fut_stop = asyncio.ensure_future(evt_stop.wait())
	pending.add(fut_stop)
	
	set_running = set()
	
	while pending:
		
		success, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
		
		do_cancel = False
		
		for fut_success in success:
			
			fut_result = fut_success.result()
			
			if fut_success is fut_stop:
				
				logger.info(f'evt_stop was SET')
				
				do_cancel = True
				
				for uid, component in d_components.items():
					component.stop('runner')
			
			elif fut_success is fut_new_q:
				
				lst_q = [fut_result]
				while not q_components_pull.empty():
					lst_q.append(q_components_pull.get_nowait())
				
				fut_new_q = asyncio.ensure_future(q_components_pull.get())
				pending.add(fut_new_q)
				
				for msg_def in lst_q:
					
					action_type, action_data = msg_def
					
					logger.info(f'action_type={action_type} | action_data={str(action_data)[:100] + ""}')
					
					if action_type == 'LC_BEGIN':
						
						uid = action_data['uid']
						
						try:
							
							d_intercepts = d_msgfwd[uid]
							
						except KeyError:
							
							funcs_msgfwd = [
								partial(register_msgfwd, d_msgfwd, uid), 
								partial(unregister_msgfwd, d_msgfwd, uid)
							]
							
							init_params = {**action_data}
							component = LiveComponent(q_components_push, init_params, funcs_msgfwd, active_mayflyrepo)
							
							d_components[uid] = component
							
							fut_run_component = asyncio.ensure_future(component.run())
							set_running.add(fut_run_component)
							pending.update(set_running)
						
						else:
							
							if len(d_intercepts) == 0:
								logger.warning(f'UNEXPECTED? d_intercepts is empty')
							
							for t_key in d_intercepts.keys():
								
								deliver_to_uid, handler_name = t_key
								
								new_component = d_components[deliver_to_uid]
								
								newdata = [handler_name, [action_type, action_data]]
								
								new_component.deliver_message(newdata)
					
					elif action_type == 'LC_END':
						
						uid = action_data['uid']
						try:
							emulated = action_data['emulated'] is True
						except KeyError:
							emulated = False
						
						try:
							
							d_intercepts = d_msgfwd[uid]
							
						except KeyError:
							
							t_component = d_components[uid]
							
							t_component.stop('emulated' if emulated else 'normal')
							
						else:
							
							if len(d_intercepts) == 0:
								logger.warning(f'UNEXPECTED? d_intercepts is empty')
							
							for t_key in d_intercepts.keys():
								
								deliver_to_uid, handler_name = t_key
								
								new_component = d_components[deliver_to_uid]
								
								newdata = [handler_name, [action_type, action_data]]
								
								new_component.deliver_message(newdata)	
					
					elif action_type == 'LC_MSG':
						
						uid  = action_data['uid']
						data = action_data['data']
						
						lst_deliverto = []
						
						try:
							
							d_intercepts = d_msgfwd[uid]
							
						except KeyError:
							
							t_component = d_components[uid]
							
							lst_deliverto.append((t_component, data))
							
						else:
							
							for t_key in d_intercepts.keys():
								
								deliver_to_uid, handler_name = t_key
								
								new_component = d_components[deliver_to_uid]
								
								newdata = [handler_name, [action_type, action_data]]
								
								lst_deliverto.append((new_component, newdata))

						for component, deliver_data in lst_deliverto:
						
							component.deliver_message(deliver_data)
					
					else:
						
						raise Exception(f'unexpected action_type : {action_type}')
			
			elif fut_success in set_running:
				
				set_running.remove(fut_success)
				
				uid = fut_result
				del d_components[uid]
		
		if do_cancel:
			
			logger.info(f'_loop_run_components DOING CANCEL')
			
			for fut in [fut_new_q]:
				fut.cancel()
				try:
					await fut
				except asyncio.CancelledError:
					pass
			
			futs_shutdown = []
			for uid, component in d_components.items():
				futs_shutdown.append(asyncio.ensure_future(_wrap_wait_shutdown(uid, component)))
			
			await asyncio.gather(*futs_shutdown)
			
			pending.clear()
			
	
	logger.info(f'_loop_run_components END')
	
	return
