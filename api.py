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
		self.root_logger.setLevel(logging.DEBUG) # or whatever
		handler = logging.FileHandler('runtime.log', 'w', 'utf-8') # or whatever
		handler.setFormatter(logging.Formatter('%(name)s %(message)s')) # or whatever
		second_handler = logging.StreamHandler(sys.stdout)
		second_handler.setLevel(logging.DEBUG)
		second_handler.setFormatter(logging.Formatter('%(name)s %(message)s'))
		self.root_logger.addHandler(handler)
		self.root_logger.addHandler(second_handler)


	#Query all info about an object with a given id and type
	#obj_id is the object's id
	#obj_type is the object's type
	#uid is the object's uid (do uid implementation later)
	#SELECT obj FROM " + obj_type + " WHERE obj->>'id'='" + obj_id + "'
	def query_current_node_info(self, obj_id, obj_type, uid=None, show_SQL = False):
		query_str = "SELECT obj FROM " + str(obj_type) + " WHERE obj->>'id'='" + str(obj_id) + "'"
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

	def get_tables(self):
 		build = 'SELECT * FROM pg_catalog.pg_tables WHERE schemaname != \'pg_catalog\' AND schemaname != \'information_schema\';'
 		self.cur.execute(build)
 		total = self.cur.fetchall()
 		table_list = []
 		for a in total:
 			table_list.append(a[1])
 		return table_list
	


	def get_node_info(self, obj_id, obj_type, output = {}, pointer = None):
		try:
			query_str = "SELECT obj->>'name', obj->>'status', obj->>'deleted' FROM " + obj_type + " WHERE obj->>'id'='" + str(obj_id) + "'"
			#self.cur.execute(query_str)
			#print(self.cur.fetchall(), obj_type, obj_id)
			self.cur.execute(query_str)
			result = self.cur.fetchall()[0]
			self.queries[query_str] = True
			return result
		except Exception as e:
			self.root_logger.info(obj_type + " " + str(obj_id) + " (POINTED TO BY " + pointer + ") DID NOT PARSE, POSSIBLY DOES NOT EXIST IN DATABASE")
			raise


	def find_nearby_nodes_df_graph(self, obj_limit, obj, output = {}):
		if len(self.existing_nodes) >= obj_limit:
			return output
		if len(self.existing_nodes) == 0:
			output = {}
			i = self.get_node_info(obj.split()[1], obj.split()[0])
			output[0] = {'pointers_from': [], 'type': obj.split()[0], 'id': obj.split()[1], 'name': i[0], 'status': i[1], 'deleted': i[2]}
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
					
					output[this_index] = {'pointers_from': [current], 'type': obj_type, 'id': result[0][0], 'name': info[0], 'status': info[1], 'deleted': info[2]}
					self.find_nearby_nodes_df_graph(obj_limit, (obj_type + " " + str(result[0][0])), output)
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
							
							output[current_index] = {'pointers_from': [current], 'type': obj_type, 'id': r, 'name': information[0], 'status': information[1], 'deleted': information[2]}
							self.find_nearby_nodes_df_graph(obj_limit, (obj_type + " " + str(r)), output)

				except Exception as e:
					pass

		try:
			for obj_type in self.pointed_to_by[parts[0]]:
				if parts[0] == 'user_' or parts[0] == 'order_':
					sql_query = "SELECT obj->>'id', obj->>'name', obj->>'status', obj->>'deleted' FROM " + obj_type + " WHERE obj->>'" + parts[0] + "id'='" + parts[1] + "'"
				else:
					sql_query = "SELECT obj->>'id', obj->>'name', obj->>'status', obj->>'deleted' FROM " + obj_type + " WHERE obj->>'" + parts[0] + "_id'='" + parts[1] + "'"
				
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
								output[this_obj_index] = {'pointers_from': [], 'type': obj_type, 'id': r[0], 'name': r[1], 'status': r[2], 'deleted': r[3]}
							self.find_nearby_nodes_df_graph(obj_limit, (obj_type + " " + str(r[0])), output)
		except Exception as e:
			pass

		return output

	def find_nearby_nodes_bf_graph(self, objs, dep_limit = 2, output = {}, obj_limit = 100):
		self.layers += 1
		#pprint(output)
		#pprint(self.existing_nodes)
		if self.layers > dep_limit or len(objs) == 0:
			self.root_logger.info('LAYER ' + str(self.layers - 1) + ' DONE\n')
			if len(objs) == 0:
				self.root_logger.info('ALL CONNECTED OBJECTS FOUND')
			return output
		
		if (self.layers == 1):
			output = {}
			i = self.get_node_info(objs[0].split()[1], objs[0].split()[0])
			output[0] = {'pointers_from': [], 'type': objs[0].split()[0], 'id': objs[0].split()[1], 'name': i[0], 'status': i[1], 'deleted': i[2]}
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
						#pprint(info)
						working_objects.append(obj_type + " " + result[0][0])
						success_counter += 1
						#pprint(self.existing_nodes)
						self.existing_nodes[working_objects[-1]] = current + success_counter
						#pprint(self.existing_nodes)
						if (current + success_counter) in output:
							output[current + success_counter]['pointers_from'].append(self.existing_nodes[obj])
						else:
							#info = self.get_node_info(result[0][0], obj_type)

							#print(info)
							output[current + success_counter] = {'pointers_from': [self.existing_nodes[obj]], 'id': result[0][0], 'type': obj_type, 'name': info[0], 'status': info[1], 'deleted': info[2]}

						if len(self.existing_nodes) >= obj_limit:
							self.root_logger.info("OBJECT LIMIT REACHED")
							return output
					except Exception as e:
						#print()
						#print(current)
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
								#print(information, current)
								working_objects.append(obj_type + " " + r)
								success_counter += 1
								self.existing_nodes[working_objects[-1]] = current + success_counter
								if (current + success_counter) in output:
									output[self.existing_nodes[obj_type + " " + r]]['pointers_from'].append(self.existing_nodes[obj])
								else:
									
									output[current + success_counter] = {'pointers_from': [self.existing_nodes[obj]], 'id': r, 'type': obj_type, 'name': information[0], 'status': information[1], 'deleted': information[2]}
								if len(self.existing_nodes) >= obj_limit:
									self.root_logger.info("OBJECT LIMIT REACHED")
									return output
					except Exception as e:
						pass
			try:
				for obj_type in self.pointed_to_by[obj.split()[0]]:
					if parts[0] == 'user_' or parts[0] == 'order_':
						sql_query = "SELECT obj->>'id', obj->>'name', obj->>'status', obj->>'deleted' FROM " + obj_type + " WHERE obj->>'" + parts[0] + "id'='" + parts[1] + "'"
					else:
						sql_query = "SELECT obj->>'id', obj->>'name', obj->>'status', obj->>'deleted' FROM " + obj_type + " WHERE obj->>'" + parts[0] + "_id'='" + parts[1] + "'"
					
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
									output[current + success_counter] = {'pointers_from': [], 'id': r[0], 'type': obj_type, 'name': r[1], 'status': r[2], 'deleted': r[3]}
								if len(self.existing_nodes) >= obj_limit:
									self.root_logger.info("OBJECT LIMIT REACHED")
									return output

			except Exception as e:
				#print(e)
				#print(current)
				pass

		return self.find_nearby_nodes_bf_graph(working_objects, dep_limit, output, obj_limit)


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
	# file = flask.request.args.get('filePath')
	test = ObjectTree(url, 'connections.txt')
	if df:
		output = test.find_nearby_nodes_df_graph(obj_limit, (obj_type + " " + obj_id))
	else:
		output = test.find_nearby_nodes_bf_graph(np.array([obj_type + " " + obj_id]), depth_limit, obj_limit=obj_limit)
	test.cur.close()
	test.con.close()
	test.root_logger.info(str(len(output)) + " OBJECTS FOUND")
	test.root_logger.info('SENDING RESPONSE')
	return flask.jsonify({'network': output, 'sqlQueries': test.queries})

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
