
import zmq
import random
import time
import math
from queue import PriorityQueue
import sys
import hashlib
import base64
import os
import numpy as np

def leer(filename):
	filename=filename+".txt"
	with open (filename, "rb") as f:
		contents = f.read()
		c= base64.encodebytes(contents)
		d={"contents": c.decode('ascii')}
		f.close()
	return contents


tam = 1024*1024*10

def store(message, name):
#if not path.exists(name):
	name = name+".txt"
	with open(name, "wb") as f:
		f.write(message)

class node:

	def __init__ (self, id, bits,port_client,succ_port, ant_port, first, my_ip):

		self.bits = bits
		self.id = id_
		self.port_client= port_client
		self.succ_port = succ_port
		self.ant_port = ant_port
		self.range = [0]*2
		self.pred_id = id_
		self.suc_id = id_
		self.first = first
		self.contenido=[]
		self.finger_table = [[0 for col in range(3)] for row in range(bits)]
		self.limit = int(math.pow(2,self.bits))
		self.init = False
		self.my_ip=my_ip
		self.cont=0


	def get_range(self):
		#print(math.pow(2, self.bits))
		if self.pred_id == int((math.pow(2,self.bits)-1)):
			self.range[0]=0
		else:
			self.range[0]=int(self.pred_id)+1
		self.range[1]=self.id

		print("Desde: " + str(self.range[0]) +  " hasta: " + str(self.range[1]))


	def set_suc(self, id_):
		self.suc_id = id_

	def set_ant(self, id_):
		self.pred_id=id_

	def belong(self, buscado):
		#bel = False
		print("El buscado es: " +str(buscado))

		if self.pred_id < self.id:
			if buscado >self.pred_id and buscado <= self.id:
				return True
		else:
			if buscado >self.pred_id or (buscado>=0 and buscado <=self.id):
				return True
		return False

	def closer(self, tgt_abs, tgt_rel):
		dir = 0
		diference = "Nada"
		npy = np.asarray(self.finger_table)
		# if self.belong(tgt):
		# 	return self.id, "tcp://localhost:"+ self.ant_port
		#
		tgt = tgt_rel
		npy_ = npy[:,0].astype(int)
		npy_ = npy_.tolist()
		print(type(self.finger_table[0][0]))
		print(type(npy_[0]))
		print(npy_)
		if self.finger_table[0][1]>tgt and self.finger_table[0][1] >n.id:
			print("Este es el caso")
			id_obj = self.finger_table[0][1]
			dir = self.finger_table[0][2]
			return id_obj, dir
		elif tgt in npy_:
			#print("aquiiiiiiiiiiiii")
			pos = npy_.index(int(tgt))
			print(pos)
			id_obj=self.finger_table[pos][1]
			dir = self.finger_table[pos][2]
			return id_obj, dir

		elif n.id > self.finger_table[0][1] and tgt < self.finger_table[0][1]:
			id_obj = self.finger_table[0][1]
			dir = self.finger_table[0][2]
			return id_obj, dir

		else:
			id_obj = self.finger_table[self.bits-1][1]
			dir = self.finger_table[self.bits-1][2]
			diference = tgt - id_obj
			ant = self.finger_table[0][1]
			for i in range (self.bits):
				if ant>self.finger_table[i][1]:
					tgt = tgt_rel
				ant=self.finger_table[i][1]

				if self.finger_table[i][1]<int(tgt):
					pos_dif = tgt-self.finger_table[i][0]
					if pos_dif<diference:
						diference=pos_dif
						print("La diferencia es: " +str(diference))
						id_obj=self.finger_table[i][1]
						dir = self.finger_table[i][2]
		return id_obj, dir





if __name__ == "__main__":

	if len(sys.argv)!=8:
		print ("No se han introducido los parámetros correctos")
		exit()

	else:
		print(sys.argv)
		id_ = int(sys.argv[1])
		bits = int(sys.argv[2])
		port_client = sys.argv[3]
		succ_port = sys.argv[4]
		ant_port = sys.argv[5]
		my_ip = sys.argv[6]
		first = sys.argv[7]

	n = node(id_ , bits , port_client, succ_port, ant_port, first, my_ip)
	ctx = zmq.Context()

	comu_ant = ctx.socket(zmq.REQ)
	ant = ctx.socket(zmq.REP)
	suc = ctx.socket(zmq.REQ)
	client = ctx.socket(zmq.REP)
	ant_dir = "tcp://*:" +n.ant_port
	client_dir = "tcp://*:" +n.port_client
	suc_dir = "tcp://"+n.succ_port
	dir_ant_node = "tcp://"+n.my_ip + ":" + n.ant_port

	ant.bind(ant_dir)
	client.bind(client_dir)
	suc.connect(suc_dir)
	found = False
	poller = zmq.Poller()

	if n.first == "Si":
		m={"action": "comenzar",  "ant": n.id, "ant_port":dir_ant_node}
		suc.send_json(m)
	elif n.first == "No":
		while (not found):
			suc.connect(suc_dir)
			m={"action": "preguntar", "id": n.id}
			suc.send_json(m)
			print("en el while")
			r = suc.recv_json()
			if r["rta"]=="No":
				print("estoy conectado a: " + suc_dir)
				suc.disconnect(suc_dir)
				suc.close()
				suc_dir = r["next"]
				suc = ctx.socket(zmq.REQ)
				print("Me conectaré a:" + suc_dir)

			elif r["rta"]=="Si":
				print("Lo encontré: " +suc_dir)
				found=True
				m={"action": "reportarse",  "ant": n.id, "ant_port":dir_ant_node}
				suc.send_json(m)
	poller.register(ant, zmq.POLLIN)
	poller.register(client, zmq.POLLIN)
	poller.register(suc, zmq.POLLIN)




	cont=1
	while True:

		print(n.finger_table)
		print("Mi antesesor es: " + dir_ant_node)
		print("Mi sucesor es: " + suc_dir)
		n.get_range()
		print("waiting")
		socks = dict(poller.poll())
		if ant in socks and socks[ant] == zmq.POLLIN:
			message = ant.recv_json()
			if message["action"]=="preguntar":
				num = int(message["id"])
				r = {}
				if n.belong(num):
					r["rta"]="Si"
					print("Me pertence")
				else:
					r["rta"]="No"
					r["next"]=suc_dir
					print("No me pertenence")
				ant.send_json(r)

			elif message["action"]== "comenzar":
				ant.send_json({"accion": "comenzar" ,"suc": n.id, "ant": n.pred_id, "ant_port":dir_ant_node})
				n.set_ant(int(message["ant"]))


			elif message["action"]=="reportarse":
				print("entrando")
				print(message["ant"])
				ant.send_json({"accion": "expandir", "suc": n.id, "ant": n.pred_id, "ant_port":dir_ant_node})
				n.set_ant(int(message["ant"]))
				own_port = "tcp:localhost:"+n.ant_port
				#print("Me llegó: " + str(message["ant_port"]))
				if own_port != message["ant_port"]:
					dir_ant_node = message["ant_port"]
			elif message["action"]=="conocer":
				lista=[]
				for element in n.contenido:
					num = (int(element, 16)%n.limit)
					print("iterando sobre: " + element)
					if not n.belong(num):
						lista.append(element)
				ant.send_json({"lista":lista})

			elif message["action"]=="traspasar":
				element=message["nombre"]
				file = leer(element)
				n.contenido.remove(element)
				name=element+".txt"
				os.remove(name)
				c= base64.encodebytes(file)
				d={"contents": c.decode('ascii')}
				ant.send_json(d)

			elif message["action"]=="client_port":
				#print("Me están preguntando")
				print(port_client)
				print(ant_port)
				port_client_= "tcp://"+n.my_ip+":"+n.port_client
				ant.send_json({"client_port":port_client_})

			elif message["action"]=="reportar_suc":

				new_suc_dir=message["dir_new_suc"]
				print(new_suc_dir)

				suc.disconnect(suc_dir)
				suc.close()
				suc = ctx.socket(zmq.REQ)

				suc_dir=new_suc_dir
				suc.connect(new_suc_dir)

				print(message["new_suc_id"])
				n.set_suc(int(message["new_suc_id"]))
				#print(suc_dir)
				n.init=True
				ant.send_string("ok")

			elif message["action"]=="actualizar":
				print("actualizando")
				temp_suc_dir=suc_dir
				ant.send_string("actualizando")
				suc.disconnect(suc_dir)
				for i in range(n.bits):
					posible_next = (n.id+(int((math.pow(2,i)))))%n.limit
					posible_next_abs = (n.id+(int((math.pow(2,i)))))
					found=False
					while not found:

						suc.connect(temp_suc_dir)
						suc.send_json({"action": "finger", "target_rel":posible_next, "target_abs": posible_next_abs})
						r=suc.recv_json()
						print(r)
						if r["found"]=="Si":
							n.finger_table[i][0]=(n.id+(int((math.pow(2,i)))))%n.limit
							n.finger_table[i][1]=int(r["id"])
							n.finger_table[i][2]=r["dir"]
							found=True
						elif r["found"]=="No":
							suc.disconnect(temp_suc_dir)
							temp_suc_dir = r["dir"]
							if temp_suc_dir == "tcp://"+ n.my_ip+ ":"+ n.ant_port:
								temp_suc_dir = suc_dir
								found=True
								suc.connect(temp_suc_dir)

				print(temp_suc_dir)
				suc.disconnect(temp_suc_dir)
				suc.connect(suc_dir)
				print("EL ID DE PARADA ES: " + str(message["id"]))
				if int(message["id"])!=n.id:
					print("acaaaaaaaaaaa")

					comu_ant.connect(dir_ant_node)
					comu_ant.send_json({"action": "actualizar", "id":message["id"]})
					comu_ant.recv_string()


			elif message["action"]=="finger":
				num_abs = int(message["target_abs"])
				num_rel = int(message["target_rel"])
				print("fingeeeeeeer")
				print(message)
				r = {}

				if n.belong(num_rel):
					r["found"]="Si"
					r["dir"]="tcp://"+n.my_ip+":"+n.ant_port
					r["id"]=n.id
					print("Me pertence")
				else:
					r["found"]="No"
					id,dir = n.closer(num_abs, num_rel)

					if dir=="tcp://"+n.my_ip+":"+n.ant_port:
						dir=suc_dir
					r["dir"]=dir

					#print("lo remito en su finger")
					print("Lo remito a: " + r["dir"])

				ant.send_json(r)



		if suc in socks and socks[suc] == zmq.POLLIN:
			m = suc.recv_json()
			print(m)
			if m["accion"]=="comenzar":
				id_suc = m["suc"]
				n.set_ant(int(id_suc))
				n.set_suc(int(id_suc))
				dir_ant_node = "tcp://" + n.succ_port
				#dir_ant_node = m["ant_port"]
				print("nuevo: "  + dir_ant_node)
				for i in range(n.bits):
					posible = (n.id+(int((math.pow(2,i)))))%n.limit
					if n.belong(posible):
						#n.finger_table=n.id
						n.finger_table[i][0]=(n.id+(int((math.pow(2,i)))))%n.limit
						n.finger_table[i][1]=n.id
						n.finger_table[i][2]="tcp://"+n.my_ip+":"+n.ant_port
					else:
						n.finger_table[i][0]=(n.id+(int((math.pow(2,i)))))%n.limit
						n.finger_table[i][1]=n.suc_id
						n.finger_table[i][2]=suc_dir


				print(n.finger_table)

			elif m["accion"]=="expandir":
				id_suc = m["suc"]
				id_pred = m["ant"]
				if id_suc == id_pred:
					n.set_suc(int(id_suc))
				else:
					n.set_ant(int(id_pred))
					n.set_suc(int(id_suc))
				own_port = "tcp://"+n.my_ip+":"+n.ant_port
				suc.send_json({"action": "conocer"})
				rt = suc.recv_json()
				lista_traspasos = rt["lista"]
				for element in lista_traspasos:
					suc.send_json({"action": "traspasar", "nombre":element})
					message=suc.recv_json()
					contenido = message["contents"].encode('ascii')
					contenido= base64.decodebytes(contenido)

					store(contenido,element)
					n.contenido.append(element)
				print("llegué acá")
				suc.disconnect(suc_dir)
				temp_suc_dir=suc_dir

				for i in range(n.bits):
					posible_next = (n.id+(int((math.pow(2,i)))))%n.limit
					posible_abs = n.id+(int((math.pow(2,i))))
					print()
					found=False
					while not found:
						suc.connect(temp_suc_dir)
						suc.send_json({"action": "finger", "target_rel":posible_next, "target_abs": posible_abs})
						r=suc.recv_json()
						if r["found"]=="Si":
							n.finger_table[i][0]=posible_next
							n.finger_table[i][1]=r["id"]
							n.finger_table[i][2]=r["dir"]
							found=True
						elif r["found"]=="No":
							suc.disconnect(temp_suc_dir)
							temp_suc_dir = r["dir"]

				suc.disconnect(temp_suc_dir)
				suc.connect(suc_dir)




				print(n.finger_table)

				print("Me llegó respuesta de mi sucesor")
				#print(dir_ant_node)

				if own_port != m["ant_port"]:
					#print("aca si es igual")
					dir_ant_node = m["ant_port"]
				#print(own_port)
				comu_ant.connect(dir_ant_node)
				print("Lo voy a enviar a: " + dir_ant_node)
				new_suc_id = "tcp://"+n.my_ip+":"+n.ant_port
				comu_ant.send_json({"action": "reportar_suc", "dir_new_suc": new_suc_id, "new_suc_id": n.id})
				comu_ant.recv_string()
				comu_ant.send_json({"action": "actualizar", "id":n.id})
				comu_ant.recv_string()


		if client in socks and socks[client] == zmq.POLLIN:
			print("entro aquí")
			message = client.recv_json()
			if message["accion"]=="preguntar":
				num = int(message["numero"])
				print(num)
				it = int(message["it"])
				print("En la it:" +str(it))
				msj = {"desde": n.range[0], "hasta": n.range[1]}
				msj = {}
				if n.belong(num):
					print("holaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
					msj["found"]="Encontrado"
					client.send_json(msj)
				else:

					print(suc_dir)
					#suc.recv_string()
					id,next = n.closer(num,num)
					print("El más cercano es: " + next)
					if next == "tcp://"+n.my_ip + ":" + n.ant_port:
						print("El mismo")
						next = suc_dir
					suc.disconnect(suc_dir)
					suc.connect(next)
					suc.send_json({"action": "client_port"})
					d=suc.recv_json()
					client_next = d["client_port"]
					print(client_next)
					msj["found"]="No"
					msj["next"]=client_next
					msj["Port"]=n.id
					client.send_json(msj)
					suc.disconnect(next)
					suc.connect(suc_dir)
			elif message["accion"]=="bajar":
				filename=message["hash"]
				print(filename)
				parte = leer(filename)
				c= base64.encodebytes(parte)
				d={"contents": c.decode('ascii')}
				client.send_json(d)


			elif message["accion"]=="guardar":
				print("Van a guardar")
				contenido = message["contents"].encode('ascii')
				contenido= base64.decodebytes(contenido)
				nombre=message["nombre"]
				numero = str(message["Numero"])
				store(contenido,nombre)
				n.contenido.append(nombre)
				client.send_string("ok")
