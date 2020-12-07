import zmq
import random
import time
import math
from queue import PriorityQueue
import sys
import hashlib
import base64

tam = 1024

def store(message, name):
#if not path.exists(name):
	print(name)
	with open(name, "ab") as f:
		f.write(message)

def hashear (file):

    file_hash=hashlib.sha1(file)
    return file_hash.hexdigest()

if __name__ == "__main__":
    ctx = zmq.Context()
    client = ctx.socket(zmq.REQ)
    cont=0
    while(True):

        accion = input("Ingrese acción: ")
        n = input("Ingrese número de puerto: ")
        node_dir = "tcp://"+n
        client.connect(node_dir)

        if accion=="Bajar":
            fichero = input("Ingrese fichero con los hashes: ")
            conta=0
            while True:


                with open(fichero, 'r') as f:
                    nombre_archivo= ""
                    for line in f:
                        found = False

                        line=line.rstrip()
                        if conta==0:
                            nombre_archivo=line
                            found = True
                            line="be"
                        conta+=1
                        print(line)
                        numero = (int(line, 16)%32)
                        while(not found):
                            client.connect(node_dir)
                            client.send_json({"accion": "preguntar", "numero": numero, "it":cont})
                            d=client.recv_json()
                            if d["found"]=="Encontrado":
                                d={"accion": "bajar","hash":line}
                                client.send_json(d)
                                r= client.recv_json()
                                contenido_recibido = r["contents"].encode('ascii')
                                p= base64.decodebytes(contenido_recibido)
                                store(p,nombre_archivo)
                                client.disconnect(node_dir)
                                found = True
                            elif d["found"]=="No":
                                #port_dir=d["next"]
                                client.disconnect(node_dir)
                                node_dir=d["next"]

                                failed_port=d["Port"]
                        print("Salí")
                break


        if accion=="Subir":
            filename = input("Ingrese nombre de archivo: ")
            pos=0
            filename_fichero = "r-"+filename+".txt"
            filename_download = "d-"+filename
            with open (filename_fichero, "a") as w:
                w.write(filename_download)
                w.write("\n")
                w.close()

            while True:
                found = False
                with open(filename, "rb") as f:
                    print("posición" + str(pos))
                    f.seek(pos,0)
                    m = f.read(tam)
                    #print(m)
                    if not m:
                        break
                    else:
                        name = hashear(m)
                        with open(filename_fichero, "a")as w:
                            w.write(name)
                            w.write("\n")
                            w.close()

                        print("el nombre es: " + name)
                        numero = (int(name, 16)%32)
                        print("El número es: " +str(numero))
                        pos+=tam
                        while(not found):
                            print(node_dir)
                            client.connect(node_dir)
                            client.send_json({"accion": "preguntar", "numero": numero, "it":cont})
                            d=client.recv_json()
                            #print(d)
                            if d["found"]=="Encontrado":
                                contents = f.read()
                                c= base64.encodebytes(m)
                                #print("el nombre es: " + name)
                                d={"accion": "guardar", "contents": c.decode('ascii'), "nombre":name, "Numero":numero}
                                client.send_json(d)
                                r = client.recv_string()

                                client.disconnect(node_dir)
                                found = True
                            elif d["found"]=="No":
                                port_dir=d["next"]
                                client.disconnect(node_dir)
                                node_dir=port_dir

                                failed_port=d["Port"]
                            cont+=1
                    print("Salí")
