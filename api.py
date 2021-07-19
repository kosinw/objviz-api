import flask
import flask_api
import psycopg2 as pcg2
import numpy as np
from pprint import pprint
import json
import traceback


class ObjectTree:
	#Initialize ObjectTree 
	#Establish database connection and cursor objects
	#database_url is the path or url to database
	def __init__(self, database_url, file_path):
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
					self.pointers_to[key].append(value)
				except:
					self.pointers_to[key] = [value]
				try:
					self.pointed_to_by[value].append(key)
				except:
					self.pointed_to_by[value] = [key]
		self.queries = {}
		#pprint(self.pointers_to, self.pointed_to_by)


	#Query all info about an object with a given id and type
	#obj_id is the object's id
	#obj_type is the object's type
	#uid is the object's uid (do uid implementation later)
	#SELECT obj FROM " + obj_type + " WHERE obj->>'id'='" + obj_id + "'
	def query_current_node_info(self, obj_id, obj_type, uid=None, show_SQL = False):
		query_str = "SELECT obj FROM " + str(obj_type) + " WHERE obj->>'id'='" + str(obj_id) + "'"
		#query_str = "SELECT jsonb_object_agg(key, value) FROM jsonb_each(SELECT obj FROM " + str(obj_type) + " WHERE obj->>'id'='" + str(obj_id) + "') AS x WHERE key LIKE '%id' AND jsonb_typeof(value) != 'null'"
		self.cur.execute(query_str)
		result = self.cur.fetchall()
		if show_SQL:
			print("SQL QUERY: " + query_str)
		return result[0][0]

	#gets object type based on key
	#A work in progress to say the best
	#key is the key that the object type is to be extracted from
	def key_to_obj_type(self, key):
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

 	#Prints all useful information about an object (can add more things later if needed)
	#info is the dictionary/json entry from the database that describes this object
	#counter is the current count
	#obj_type is the type of this object
	#obj_id is the id of this object
	#pointer is the object that points to this object
	def node_info_to_dict(self, info, counter, obj_type, obj_id, pointer, running_dict):
		running_dict[counter] = {
			"type" : obj_type,
			"id" : obj_id,
			"pointers_from" : [pointer]
		}

		for key in info.keys():
			if key in ['name', 'type_full', 'status', 'deleted']:
				running_dict[counter][key] = info[key]
		return running_dict

	def get_tables(self):
 		build = 'SELECT * FROM pg_catalog.pg_tables WHERE schemaname != \'pg_catalog\' AND schemaname != \'information_schema\';'
 		self.cur.execute(build)
 		total = self.cur.fetchall()
 		table_list = []
 		for a in total:
 			table_list.append(a[1])
 		return table_list
	
	#IGNORE THIS		
	def find_nested_id(self, obj, key):
	    if (key.endswith('id') or key.endswith('ids')) and key.endswith('uid') == False and key.endswith('uids') == False and key != 'id' and key in obj: 
	    	return key
	    for k, v in obj.items():
	        if isinstance(v,dict):
	            item = find_nested_id(v, key)
	            if item is not None:
	                return item

	#Finds nodes that connect to the current node (recursive)
	#limit is limit on number of nodes
	#obj_id is the object's id
	#obj_type is the object's type
	#uid is the object's uid (do uid implementation later)
	#pointer is the object that pointed to the current object
	def find_nearby_nodes_df(self, limit, obj_id, obj_type, uid=None, pointer = None):
		if self.iteration_counter >= limit:
			return "limit reached"
		else:
			info = self.query_current_node_info(obj_id, obj_type)
			dict_id = str(obj_type) + str(obj_id)
			
			self.iteration_counter += 1
			self.existing_nodes[dict_id] = self.iteration_counter
			current = self.iteration_counter
			self.print_node_info(info, self.iteration_counter, obj_type, obj_id, pointer)
			for key in info.keys():
				if key.endswith('id') and key.endswith('uid') == False and key != 'id':
					try:
						next_obj_type = self.key_to_obj_type(key)
						next_obj_id = info[key]
						next_dict_id = str(next_obj_type) + str(next_obj_id)
						if next_dict_id in self.existing_nodes:
							print("Node " + str(current) + " points to Node " + str(self.existing_nodes[next_dict_id]))
							raise ValueError('node already exists')
						else:
							self.find_nearby_nodes(limit, next_obj_id, next_obj_type, pointer=current)
					except Exception as e:
						pass
				elif key.endswith('ids') and key.endswith('uids') == False:
					if isinstance(info[key], dict):
	 					for next_object_id in info[key].keys():
	 						try:
	 							next_obj_type = self.key_to_obj_type(key)
	 							next_dict_id = str(next_obj_type) + str(next_object_id)
	 							if next_dict_id in self.existing_nodes:
	 								print("Node " + str(current) + " points to Node " + str(self.existing_nodes[next_dict_id]))
	 								raise ValueError('node already exists')
	 							else:
	 								self.find_nearby_nodes(limit, next_object_id, next_obj_type, pointer=current)
	 						except Exception as e:
	 							pass
					elif isinstance(info[key], list):
	 					for next_object_id in info[key]:
	 						try:
	 							next_obj_type = self.key_to_obj_type(key)
	 							next_dict_id = str(next_obj_type) + str(next_object_id)
	 							if next_dict_id in self.existing_nodes:
	 								print("Node " + str(current) + " points to Node " + str(self.existing_nodes[next_dict_id]))
	 								raise ValueError('node already exists')
	 							else:
	 								self.find_nearby_nodes(limit, next_object_id, next_obj_type, pointer=current)
	 						except Exception as e:
	 							pass

	def find_nearby_nodes_bf(self, layer_limit, objs, output, uid=None, pointer = None):
		if objs.shape[0] == 0 or self.layers > layer_limit:
			return output
		self.layers += 1
		working_objects = np.array([])
		current = self.iteration_counter
		extra_pointers = {}
		for obj in objs:
			obj_id = obj['id']
			try:
				obj_type = obj['type']
			except:
				obj_type = 'customer'
			if obj_type == 'user' or obj_type == 'order':
				obj_type += '_'
			info = self.query_current_node_info(obj_id, obj_type)
			dict_id = str(obj_type) + str(obj_id)
			self.iteration_counter += 1
			self.existing_nodes[dict_id] = self.iteration_counter
			current = self.iteration_counter
			output = self.node_info_to_dict(info, self.iteration_counter, obj_type, obj_id, pointer, output)
			#print(output)
			success_counter = 1
			for key in info.keys():
				if key.endswith('id') and key.endswith('uid') == False and key != 'id':
					try:
						next_obj_type = self.key_to_obj_type(key)
						next_obj_id = info[key]
						next_dict_id = str(next_obj_type) + str(next_obj_id)
						#print(self.existing_nodes)

						if next_dict_id in self.existing_nodes:
							#extra_pointers[current] = [1,2,3]
							
							# print("Node " + str(current) + " points to Node " + str(self.existing_nodes[next_dict_id]))
							try:
								extra_pointers[current].append(self.existing_nodes[next_dict_id])
							except:
								extra_pointers[current] = [self.existing_nodes[next_dict_id]]
							#print(extra_pointers)
							raise ValueError('node already exists')
						else:
							this_obj = self.query_current_node_info(info[key], self.key_to_obj_type(key))
							self.existing_nodes[next_dict_id] = self.iteration_counter + success_counter
							success_counter += 1
							working_objects = np.append(working_objects, this_obj)
					except Exception as e:
						pass
				elif key.endswith('ids') and key.endswith('uids') == False:
					if isinstance(info[key], dict):
						#print(info[key].keys())
						
						for next_object_id in info[key].keys():
							#print(next_object_id)
							try:
	 							next_obj_type = self.key_to_obj_type(key)
	 							next_dict_id = str(next_obj_type) + str(next_object_id)
	 							#print(self.existing_nodes)
	 							if next_dict_id in self.existing_nodes:
	 								extra_pointers[current] = np.append(np.asarray(extra_pointers[current]), self.existing_nodes[next_dict_id]).tolist()
	 								raise ValueError('node already exists')
	 							else:
	 								#print(next_obj_id, next_obj_type)
	 								this_obj = self.query_current_node_info(next_object_id, next_obj_type)
	 								self.existing_nodes[next_dict_id] = self.iteration_counter + success_counter
	 								success_counter += 1
	 								working_objects = np.append(working_objects, this_obj)
							except Exception as e:
	 							pass
					elif isinstance(info[key], list):
	 					for next_object_id in info[key]:
	 						try:
	 							next_obj_type = self.key_to_obj_type(key)
	 							next_dict_id = str(next_obj_type) + str(next_object_id)
	 							if next_dict_id in self.existing_nodes:
	 								extra_pointers[current] = np.append(np.asarray(extra_pointers[current]), self.existing_nodes[next_dict_id]).tolist()
	 								raise ValueError('node already exists')
	 							else:
	 								this_obj = self.query_current_node_info(next_obj_id, next_obj_type)
	 								self.existing_nodes[next_dict_id] = self.iteration_counter + success_counter
	 								success_counter += 1
	 								working_objects = np.append(working_objects, this_obj)
	 						except Exception as e:
	 							pass

		#print(working_objects)
		#print(extra_pointers)
		for key in extra_pointers:
			for pointer in extra_pointers[key]:
				output[pointer]['pointers_from'].append(key)
		return self.find_nearby_nodes_bf(layer_limit, working_objects, output, pointer=current)

	def get_name(self, obj_id, obj_type):
		try:
			query_str = "SELECT obj->>'name' FROM " + obj_type + " WHERE obj->>'id'='" + str(obj_id) + "'"
			#self.queries = np.append(self.queries, query_str)
			self.cur.execute(query_str)
			result = self.cur.fetchall()
			return result[0][0]
		except Exception as e:
			pass

	def find_nearby_nodes_bf_graph(self, objs, dep_limit, output = {}):
		if self.layers > dep_limit or objs.shape[0] == 0:
			return output
		self.layers += 1
		if (self.layers == 1):
			output[0] = {'pointers_from': [], 'type': objs[0].split()[0], 'id': objs[0].split()[1], 'name': self.get_name(objs[0].split()[1], objs[0].split()[0])}
			self.existing_nodes[objs[0]] = 0
		working_objects = np.array([])
		for obj in objs:
			current = self.existing_nodes[obj]
			success_counter = len(output) - 1 - current
			# try:
				

			# except KeyError as err:
  	# 			print("Unknown key: ", err)
  	# 			traceback.print_exc()
			
			
			parts = obj.split()
			#print(self.pointers_to[obj.split()[0]])
			for obj_type in self.pointers_to[obj.split()[0]]:
				sql_query = "SELECT obj->>'" + obj_type + "_id' FROM " + parts[0] + " WHERE obj->>'id'='" + parts[1] + "'"
				#print(sql_query)
				self.queries[sql_query] = True
				self.cur.execute(sql_query)
				result = self.cur.fetchall()
				#print(len(result))
				#print(result)
				#print(result)
				if len(result) != 0 and result[0][0] != None and (obj_type + " " + result[0][0]) in self.existing_nodes:

					if self.existing_nodes[obj] not in output[self.existing_nodes[obj_type + " " + result[0][0]]]['pointers_from']:
						output[self.existing_nodes[obj_type + " " + result[0][0]]]['pointers_from'].append(self.existing_nodes[obj])
				elif len(result) != 0 and result[0][0] != None:

					#check if already in existing_nodes
					working_objects = np.append(working_objects, obj_type + " " + result[0][0])
					success_counter += 1
					self.existing_nodes[working_objects[-1]] = current + success_counter
					if (current + success_counter) in output:
						#print (obj, obj_type, current, current + success_counter, output[current+success_counter])
						output[current + success_counter]['pointers_from'].append(self.existing_nodes[obj])
					else:
						output[current + success_counter] = {'pointers_from': [self.existing_nodes[obj]], 'id': result[0][0], 'type': obj_type, 'name': self.get_name(result[0][0], obj_type)}

						# ['pointers_from'] = [str(self.existing_nodes[obj])]
						# output[size]['id'] = result[0][0]
						# output[size]['type'] = obj_type
				else:
					try:
						sql_query = "SELECT obj->>'" + obj_type + "_ids' FROM " + parts[0] + " WHERE obj->>'id'='" + parts[1] + "'"
						self.cur.execute(sql_query)
						#print(sql_query)
						self.queries[sql_query] = True
						results = json.loads(self.cur.fetchall()[0][0]).keys()
						#print(len(results))
						#print(results)
						for r in results:
							#print(r)
							if (obj_type + " " + r) in self.existing_nodes:
								if self.existing_nodes[obj] not in output[self.existing_nodes[obj_type + " " + r]]:
									output[self.existing_nodes[obj_type + " " + r]]['pointers_from'].append(self.existing_nodes[obj])
							else:
								working_objects = np.append(working_objects, obj_type + " " + r)
								success_counter += 1
								self.existing_nodes[working_objects[-1]] = current + success_counter
								if (current + success_counter) in output:

									output[self.existing_nodes[obj_type + " " + r]]['pointers_from'].append(self.existing_nodes[obj])
								else:
									output[current + success_counter] = {'pointers_from': [self.existing_nodes[obj]], 'id': r, 'type': obj_type, 'name': self.get_name(r, obj_type)}
									# output[size]['pointers_from'] = [self.existing_nodes[obj]]
									# output[size]['id'] = result[0][0]
									# output[size]['type'] = obj_type
					except Exception as e:
						pass
			try:
				#print(self.pointed_to_by[obj.split()[0]])
				for obj_type in self.pointed_to_by[obj.split()[0]]:
					sql_query = "SELECT obj->>'id' FROM " + obj_type + " WHERE obj->>'" + parts[0] + "_id'='" + parts[1] + "'"
					#print(sql_query)
					self.queries[sql_query] = True
					self.cur.execute(sql_query)
					results = self.cur.fetchall()
					#print(len(results))
					#print(results)
					for r in results:
						if r[0] != None:
							#print('still running' + str(len(results)))
							if (obj_type + " " + r[0]) in self.existing_nodes:
								if self.existing_nodes[obj_type + " " + r[0]] not in output[current]['ponters_from']:
									output[current]['pointers_from'].append(self.existing_nodes[obj_type + " " + r[0]])
							else:
								working_objects = np.append(working_objects, obj_type + " " + r[0])
								success_counter += 1
								self.existing_nodes[working_objects[-1]] = current + success_counter
								output[current]['pointers_from'].append(current + success_counter)
								if (current + success_counter) not in output:
									output[current + success_counter] = {'pointers_from': [], 'id': r[0], 'type': obj_type, 'name': self.get_name(r[0], obj_type)}

			except Exception as e:
				print(e)
		#pprint(output)
		#print(working_objects)
		return self.find_nearby_nodes_bf_graph(working_objects, dep_limit, output)


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
	depth_limit = int(flask.request.args.get('depth limit'))
	url = flask.request.args.get('uri')
	# file = flask.request.args.get('file path')
	#test = ObjectTree(url)
	#output = test.find_nearby_nodes_bf(depth_limit, np.array([test.query_current_node_info(str(obj_id), obj_type)]), {})
	test = ObjectTree(url, 'connections.txt')
	output = test.find_nearby_nodes_bf_graph(np.array([obj_type + " " + obj_id]), depth_limit)
	test.cur.close()
	test.con.close()
	return flask.jsonify({'output': output, 'SQL Queries': test.queries})

@app.route('/api/getTypes', methods=['GET'])
def return_types():
	url = flask.request.args.get('uri')
	test = ObjectTree(url, 'connections.txt')
	output = test.get_tables()
	test.cur.close()
	test.con.close()
	#print(output)
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
