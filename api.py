import flask
import flask_api
import psycopg2 as pcg2
import numpy as np
from pprint import pprint
import json
import traceback
import logging
import sys
import bisect


class ObjectTree:
	"""
	A class that generates a network of connected objects
	"""
	def __init__(self, database_url, file_path):
		"""
		Initializes an ObjectTree object
		:param database_url: The url of the database that's being connected to
		:type database_url: str
		:param file_path: The path to the file that contains the object type graph
		:type file_path: str
		"""
		self.url = database_url
		self.iteration_counter = 0
		self.con = pcg2.connect(self.url)
		self.con.autocommit = True
		self.cur = self.con.cursor()
		self.existing_nodes = {}
		self.layers = 0
		self.pointers_to = {}
		self.pointed_to_by = {}
		with open(file_path) as file:
			for line in file:
				(key, value) = line.split(" -> ")
				if value.endswith("\n"):
					value = value[:-1]
				try:
					bisect.insort(self.pointers_to[key], value)
					#self.pointers_to[key].append(value)
				except:
					self.pointers_to[key] = [value]
				try:
					bisect.insort(self.pointed_to_by[value], key)
					#self.pointed_to_by[value].append(key)
				except:
					self.pointed_to_by[value] = [key]
		self.queries = {}
		self.root_logger= logging.getLogger()
		self.root_logger.setLevel(logging.DEBUG)
		handler = logging.FileHandler('runtime.log', 'w', 'utf-8')
		handler.setFormatter(logging.Formatter('%(name)s %(message)s'))
		second_handler = logging.StreamHandler(sys.stdout)
		second_handler.setLevel(logging.DEBUG)
		second_handler.setFormatter(logging.Formatter('%(name)s %(message)s'))
		self.root_logger.addHandler(handler)
		self.root_logger.addHandler(second_handler)

	def query_current_node_info(self, obj_id, obj_type):
		"""
		Queries all of the available information about a single object
		:param obj_id: the id of the object
		:type obj_id: str or int
		:param obj_type: the type of the object
		:type obj_type: str
		:returns: all of the information about the object
		:rtype: dict
		"""
		query_str = "SELECT obj FROM " + str(obj_type) + " WHERE obj->>'id'='" + str(obj_id) + "'"
		self.cur.execute(query_str)
		result = self.cur.fetchall()
		if show_SQL:
			print("SQL QUERY: " + query_str)
		return result[0][0]

	def key_to_obj_type(self, key):
		"""
		Gets an object's type based on the field name that points to it
		:param key: the field name that points to an object type
		:type key: str
		:returns: the object type that the field name points to
		:rtype: str
		"""
		if key.endswith('ids'):
			key = key[0:-1]
		if key == 'order_id' or key == 'user_id':
			return key[0:-2]
		elif key == 'partner_id' or key == 'demand_partner_id':
			return 'account'
		elif key == 'openx_buyer_id':
			return 'buyer'
		else:
			return key[0:-3]

	def get_tables(self):
		"""
		Gets all table names from the database
		:returns: All table names from the database
		:rtype: list
		"""
		build = 'SELECT * FROM pg_catalog.pg_tables WHERE schemaname != \'pg_catalog\' AND schemaname != \'information_schema\';'
		self.cur.execute(build)
		total = self.cur.fetchall()
		table_list = []
		for a in total:
			table_list.append(a[1])
		return table_list
	


	def get_node_info(self, obj_id, obj_type, pointer = None):
		"""
		Gets only crucial information from a given object (to be displayed in object preview in visualizer)
		:param obj_id: the id of the object
		:type obj_id: int or str
		:param obj_type: the type of the object
		:type obj_type: str
		:param pointer: the object that points to the target object (in case the target object does not exist in database)
		:type pointer: str
		:returns: the name, status, deleted, and type_full fields of the object if possible. Else, raises error
		:rtype: list (tuple)
		"""
		try:
			query_str = "SELECT obj->>'name', obj->>'status', obj->>'deleted', obj->>'type_full' FROM " + obj_type + " WHERE obj->>'id'='" + str(obj_id) + "'"
			self.cur.execute(query_str)
			result = self.cur.fetchall()[0]
			self.queries[query_str] = True
			return result
		except Exception as e:
			self.root_logger.info(obj_type + " " + str(obj_id) + " (POINTED TO BY " + pointer + ") DID NOT PARSE, POSSIBLY DOES NOT EXIST IN DATABASE")
			raise


	def find_nearby_nodes_df_graph(self, obj_limit, obj, output = {}, current_depth=0):
		"""
		Recursively generates a network of objects connected to one object, searching through database connections depth-first
		:param obj_limit: the maximum number of objects to be added to the network
		:type obj_limit: int
		:param obj: the origninal object; the starting point of the network
		:type obj: str
		:param output: the running dictionary of objects as they are added to the network
		:type output: dict
		:param current_depth: the number of layers away from the starting object that the current iteration is; used for measuring greatest tree depth
		:type current_depth: int
		:returns: an indexed network of objects and their id, type, name, status, deleted, type_full, and the objects that point to them
		:rtype: dict
		"""
		if current_depth >= self.layers:
			self.layers = current_depth
		if len(self.existing_nodes) >= obj_limit:
			return output
		if len(self.existing_nodes) == 0:
			output = {}
			i = self.get_node_info(obj.split()[1], obj.split()[0])
			output[0] = {'pointers_from': [], 'type': obj.split()[0], 'id': obj.split()[1], 'name': i[0], 'status': i[1], 'deleted': i[2], 'type_full': i[3]}
			self.existing_nodes[obj] = 0

		current = self.existing_nodes[obj]
		if len(self.existing_nodes) % 25 == 0:
			self.root_logger.info(str(len(self.existing_nodes)) + " OBJECTS FOUND...")
		parts = obj.split()

		for obj_type in self.pointers_to[parts[0]]:
			if len(self.existing_nodes) >= obj_limit:
				return output
			if obj_type == 'user_' or obj_type == 'order_':
				sql_query = "SELECT obj->>'" + obj_type + "id' FROM " + parts[0] + " WHERE obj->>'id'='" + parts[1] + "'"
			else:
				sql_query = "SELECT obj->>'" + obj_type + "_id' FROM " + parts[0] + " WHERE obj->>'id'='" + parts[1] + "'"
			self.queries[sql_query] = True
			self.cur.execute(sql_query)
			result = self.cur.fetchall()
			
			if len(result) != 0 and result[0][0] != None and (obj_type + " " + result[0][0]) in self.existing_nodes:
				if current not in output[self.existing_nodes[obj_type + " " + result[0][0]]]['pointers_from']:
					output[self.existing_nodes[obj_type + " " + result[0][0]]]['pointers_from'].append(current)
			elif len(result) != 0 and result[0][0] != None:
				try:
					info = self.get_node_info(result[0][0], obj_type, pointer=obj)
					this_index = len(self.existing_nodes)
					
					self.existing_nodes[obj_type + " " + str(result[0][0])] = this_index
					
					output[this_index] = {'pointers_from': [current], 'type': obj_type, 'id': result[0][0], 'name': info[0], 'status': info[1], 'deleted': info[2], 'type_full': info[3]}
					self.find_nearby_nodes_df_graph(obj_limit, (obj_type + " " + str(result[0][0])), output, current_depth=current_depth + 1)
				except:
					pass
			else:
				try:
					if obj_type == 'user_' or obj_type == 'order_':
						sql_query = "SELECT obj->>'" + obj_type + "ids' FROM " + parts[0] + " WHERE obj->>'id'='" + parts[1] + "'"
					else:
						sql_query = "SELECT obj->>'" + obj_type + "_ids' FROM " + parts[0] + " WHERE obj->>'id'='" + parts[1] + "'"
					
					self.cur.execute(sql_query)
					results = json.loads(str(self.cur.fetchall()[0][0])).keys()
					self.queries[sql_query] = True
					for r in results:
						if (obj_type + " " + r) in self.existing_nodes:
							if self.existing_nodes[obj] not in output[self.existing_nodes[obj_type + " " + r]]['pointers_from']:
								output[self.existing_nodes[obj_type + " " + r]]['pointers_from'].append(current)

						else:
							information = self.get_node_info(r, obj_type, pointer = obj)
							current_index = len(self.existing_nodes)
							if current_index >= obj_limit:
								return output
							self.existing_nodes[obj_type + " " + r] = current_index
							
							output[current_index] = {'pointers_from': [current], 'type': obj_type, 'id': r, 'name': information[0], 'status': information[1], 'deleted': information[2], 'type_full': information[3]}
							self.find_nearby_nodes_df_graph(obj_limit, (obj_type + " " + str(r)), output, current_depth = current_depth + 1)

				except Exception as e:
					pass

		try:
			for obj_type in self.pointed_to_by[parts[0]]:
				if parts[0] == 'user_' or parts[0] == 'order_':
					sql_query = "SELECT obj->>'id', obj->>'name', obj->>'status', obj->>'deleted', obj->>'type_full' FROM " + obj_type + " WHERE obj->>'" + parts[0] + "id'='" + parts[1] + "'"
				else:
					sql_query = "SELECT obj->>'id', obj->>'name', obj->>'status', obj->>'deleted', obj->>'type_full' FROM " + obj_type + " WHERE obj->>'" + parts[0] + "_id'='" + parts[1] + "'"
				
				self.cur.execute(sql_query)
				results = self.cur.fetchall()
				self.queries[sql_query] = True

				for r in results:
					if r[0] != None:
						if (obj_type + " " + r[0]) in self.existing_nodes:
							if self.existing_nodes[obj_type + " " + r[0]] not in output[current]['pointers_from']:
								output[current]['pointers_from'].append(self.existing_nodes[obj_type + " " + r[0]])
						else:
							this_obj_index = len(self.existing_nodes)
							if this_obj_index >= obj_limit:
								return output
							self.existing_nodes[obj_type + " " + r[0]] = this_obj_index
							output[current]['pointers_from'].append(this_obj_index)
							if this_obj_index not in output:
								output[this_obj_index] = {'pointers_from': [], 'type': obj_type, 'id': r[0], 'name': r[1], 'status': r[2], 'deleted': r[3], 'type_full': r[4]}
							self.find_nearby_nodes_df_graph(obj_limit, (obj_type + " " + str(r[0])), output, current_depth = current_depth + 1)
		except Exception as e:
			pass

		return output

	def find_nearby_nodes_bf_graph(self, objs, dep_limit = 2, output = {}, obj_limit = 100):
		"""
		Recursively generates a network of objects connected to one object, searching through database connections breadth-first
		:param objs: the objects that a given iteration is looking through
		:type objs: list
		:param dep_limit: the maximum depth that the search is allowed to reach
		:type dep_limit: int
		:param output: the running dictionary of objects as they are added to the network
		:type output: dict
		:param obj_limit: the maximum number of objects that can be in the generated network
		:type obj_limit: int
		:returns: an indexed network of objects and their id, type, name, status, deleted, type_full, and the objects that point to them
		:rtype: dict
		"""
		self.layers+=1
		if self.layers > dep_limit or len(objs) == 0:
			self.root_logger.info('LAYER ' + str(self.layers - 1) + ' DONE\n')
			if len(objs) == 0:
				self.root_logger.info('ALL CONNECTED OBJECTS FOUND')
			else:
				self.layers -= 1
			return output
		
		if (self.layers == 1):
			output = {}
			i = self.get_node_info(objs[0].split()[1], objs[0].split()[0])
			output[0] = {'pointers_from': [], 'type': objs[0].split()[0], 'id': objs[0].split()[1], 'name': i[0], 'status': i[1], 'deleted': i[2], 'type_full': i[3]}
			self.existing_nodes[objs[0]] = 0
		else:
			self.root_logger.info('LAYER ' + str(self.layers - 1) + ' DONE. SEARCHING LAYER ' + str(self.layers) + '...\n')
		working_objects = []
		for obj in objs:
			current = self.existing_nodes[obj]
			success_counter = len(output) - 1 - current			
			parts = obj.split()
			for obj_type in self.pointers_to[obj.split()[0]]:
				if obj_type == 'order_' or obj_type == 'user_':
					sql_query = "SELECT obj->>'" + obj_type + "id' FROM " + parts[0] + " WHERE obj->>'id'='" + parts[1] + "'"
				else:
					sql_query = "SELECT obj->>'" + obj_type + "_id' FROM " + parts[0] + " WHERE obj->>'id'='" + parts[1] + "'"

				self.queries[sql_query] = True
				self.cur.execute(sql_query)

				result = self.cur.fetchall()
				

				if len(result) != 0 and result[0][0] != None and (obj_type + " " + result[0][0]) in self.existing_nodes:
					if self.existing_nodes[obj] not in output[self.existing_nodes[obj_type + " " + result[0][0]]]['pointers_from']:
						output[self.existing_nodes[obj_type + " " + result[0][0]]]['pointers_from'].append(self.existing_nodes[obj])
				elif len(result) != 0 and result[0][0] != None:
					try:
						info = self.get_node_info(result[0][0], obj_type, pointer = obj)
						working_objects.append(obj_type + " " + result[0][0])
						success_counter += 1
						self.existing_nodes[working_objects[-1]] = current + success_counter
						if (current + success_counter) in output:
							output[current + success_counter]['pointers_from'].append(self.existing_nodes[obj])
						else:
							output[current + success_counter] = {'pointers_from': [self.existing_nodes[obj]], 'id': result[0][0], 'type': obj_type, 'name': info[0], 'status': info[1], 'deleted': info[2], 'type_full': info[3]}

						if len(self.existing_nodes) >= obj_limit:
							self.root_logger.info("OBJECT LIMIT REACHED")
							return output
					except Exception as e:
						pass

				else:
					try:
						if obj_type == 'user_' or obj_type == 'order_':
							sql_query = "SELECT obj->>'" + obj_type + "ids' FROM " + parts[0] + " WHERE obj->>'id'='" + parts[1] + "'"
						else:
							sql_query = "SELECT obj->>'" + obj_type + "_ids' FROM " + parts[0] + " WHERE obj->>'id'='" + parts[1] + "'"
						self.cur.execute(sql_query)
						
						results = json.loads(self.cur.fetchall()[0][0]).keys()
						self.queries[sql_query] = True
						for r in results:
							if (obj_type + " " + r) in self.existing_nodes:
								if self.existing_nodes[obj] not in output[self.existing_nodes[obj_type + " " + r]]['pointers_from']:
									output[self.existing_nodes[obj_type + " " + r]]['pointers_from'].append(self.existing_nodes[obj])
							else:
								information = self.get_node_info(r, obj_type, pointer = obj)
								working_objects.append(obj_type + " " + r)
								success_counter += 1
								self.existing_nodes[working_objects[-1]] = current + success_counter
								if (current + success_counter) in output:
									output[self.existing_nodes[obj_type + " " + r]]['pointers_from'].append(self.existing_nodes[obj])
								else:
									
									output[current + success_counter] = {'pointers_from': [self.existing_nodes[obj]], 'id': r, 'type': obj_type, 'name': information[0], 'status': information[1], 'deleted': information[2], 'type_full': information[3]}
								if len(self.existing_nodes) >= obj_limit:
									self.root_logger.info("OBJECT LIMIT REACHED")
									return output
					except Exception as e:
						pass
			try:
				for obj_type in self.pointed_to_by[obj.split()[0]]:
					if parts[0] == 'user_' or parts[0] == 'order_':
						sql_query = "SELECT obj->>'id', obj->>'name', obj->>'status', obj->>'deleted', obj->>'type_full' FROM " + obj_type + " WHERE obj->>'" + parts[0] + "id'='" + parts[1] + "'"
					else:
						sql_query = "SELECT obj->>'id', obj->>'name', obj->>'status', obj->>'deleted', obj->>'type_full' FROM " + obj_type + " WHERE obj->>'" + parts[0] + "_id'='" + parts[1] + "'"
					
					self.cur.execute(sql_query)
					results = self.cur.fetchall()
					self.queries[sql_query] = True

					for r in results:
						if r[0] != None:
							if (obj_type + " " + r[0]) in self.existing_nodes:
								if self.existing_nodes[obj_type + " " + r[0]] not in output[current]['pointers_from']:
									output[current]['pointers_from'].append(self.existing_nodes[obj_type + " " + r[0]])
							else:
								working_objects.append(obj_type + " " + r[0])
								success_counter += 1
								self.existing_nodes[working_objects[-1]] = current + success_counter
								output[current]['pointers_from'].append(current + success_counter)
								if (current + success_counter) not in output:
									output[current + success_counter] = {'pointers_from': [], 'id': r[0], 'type': obj_type, 'name': r[1], 'status': r[2], 'deleted': r[3], 'type_full': r[4]}
								if len(self.existing_nodes) >= obj_limit:
									self.root_logger.info("OBJECT LIMIT REACHED")
									return output

			except Exception as e:
				pass

		return self.find_nearby_nodes_bf_graph(working_objects, dep_limit, output, obj_limit)

	def get_output_stats(self, output):
		"""
		Produces statistics about the generated network of objects, including counts/frequencies of object types and subtypes and maximum depth
		:param output: the generated network of objects
		:type output: dict
		:returns: statistics about the object network
		:rtype: dict
		"""
		type_dict = {}
		type_full_dict = {}
		total = 0
		for key in output.keys():
			try:
				type_dict[output[key]['type']] += 1
			except:
				type_dict[output[key]['type']] = 1
			try:
				type_full_dict[output[key]['type_full']] += 1
			except:
				type_full_dict[output[key]['type_full']] = 1
			total += 1
		final_stats = {}
		for key in type_dict.keys():
			final_stats[key] = {'count': type_dict[key], 'percent_of_total': (type_dict[key]/total)*100}
			for k in type_full_dict.keys():
				if k == None:
					pass
				elif k.startswith(key):
					final_stats[key][k] = {'count': type_full_dict[k], 'percent_of_total': (type_full_dict[k]/total)*100, 'percent_of_' + key + '(s)': (type_full_dict[k]/type_dict[key])*100}
		return final_stats

#print(test.query_current_node_info('1610767417', 'adunitgroup'))
#adunitgroup 1610767417
#account 537237219
#adunit 536873591
#site 1610870269
#user_ 1610612857
#deal 1610619602
#customer 497

#EMPTY TALBES:
#acl, ad_deleted, ad_deleted_bak, adunit_deleted, adunit_deleted_bak, adunitgroup_adunit_xref,
#app_category, appinfo, audiencesegment, audittrail, buyer, conversiontag_order_xref, 
#creative_deleted, creative_deleted_bak, datapull, endpoint, feecap, floorrule, lineitem_deleted,
#lineitem_deleted_bak, options, order__deleted, order__deleted_bak, partner, permissions_version,
#seat, site_deleted, site_deleted_bak, targeting_options, type_uuid_mapping, user_partner_xref


app = flask.Flask(__name__)
app.config["DEBUG"] = True



@app.route('/api/verifyURI', methods=['GET'])
def verify_connection():
	try:
		url = flask.request.args.get('uri')
		test = ObjectTree(url, 'connections.txt')
		return flask.jsonify(success=True), 200
	except:
		return flask.jsonify({"success" : False, "error" : {"type" : "InvalidDatabaseCredentials", "message" : "Could not connect to database with given credentials"}})

@app.route('/api/getNetwork', methods=['GET'])
def parse_request():
	obj_id = flask.request.args.get('id')
	obj_type = flask.request.args.get('type')
	depth_limit = int(flask.request.args.get('depthLimit'))
	try:
		obj_limit = int(flask.request.args.get('objectLimit'))
	except:
		obj_limit = 100

	if flask.request.args.get('depthFirst') in ["True", "true"]:
		df = True
	else:
		df = False
	url = flask.request.args.get('uri')
	test = ObjectTree(url, 'connections.txt')
	
	if df:
		output = test.find_nearby_nodes_df_graph(obj_limit, (obj_type + " " + obj_id))
	else:
		output = test.find_nearby_nodes_bf_graph(np.array([obj_type + " " + obj_id]), depth_limit, obj_limit=obj_limit)
	max_depth = test.layers
	test.cur.close()
	test.con.close()
	test.root_logger.info(str(len(output)) + " OBJECTS FOUND")
	stats = {'types': test.get_output_stats(output), 'max_depth': max_depth}
	test.root_logger.info('SENDING RESPONSE')

	return flask.jsonify({'network': output, 'sqlQueries': test.queries, 'statistics': stats})

@app.route('/api/getTypes', methods=['GET'])
def return_types():
	url = flask.request.args.get('uri')
	test = ObjectTree(url, 'connections.txt')
	output = test.get_tables()
	test.cur.close()
	test.con.close()
	return flask.jsonify(output)

@app.route('/api/getObjectInfo', methods=['GET'])
def get_info():
	url = flask.request.args.get('uri')
	test = ObjectTree(url, 'connections.txt')
	obj_id = flask.request.args.get('id')
	obj_type = flask.request.args.get('type')
	output = test.query_current_node_info(obj_id, obj_type)
	test.cur.close()
	test.con.close()
	return flask.jsonify(output)

if __name__ == "__main__":
	app.run()
