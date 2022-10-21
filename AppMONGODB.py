'''
Tema: APLICACION MONGO
Fecha: 17 de Octubre del 2022
Autor: FRANCISCO ISAAC SANCHEZ IBARRA
'''
from CRUD_MYSQL import MySQL
from Practica1 import Password
from MongoDB import PyMongo
from env import variables
from configuracion import varmongo


def cargar_estudiantes():
    obj_MySQL = MySQL(variables)
    obj_PyMongo = PyMongo(varmongo)
    # consultas
    sql_estudiante = "SELECT * FROM estudiantes;"
    sql_kardex = "SELECT * FROM kardex;"
    sql_usuario = "SELECT * FROM usuarios;"
    obj_MySQL.conectar_mysql()
    lista_estudiantes = obj_MySQL.consulta_sql(sql_estudiante)
    lista_kardex = obj_MySQL.consulta_sql(sql_kardex)
    lista_usuarios = obj_MySQL.consulta_sql(sql_usuario)
    obj_MySQL.desconectar_mysql()
    # insertar datos en mongo
    obj_PyMongo.conectar_mongodb()
    for est in lista_estudiantes:
        e = {
            'control': est[0],
            'nombre': est[1]
        }
        obj_PyMongo.insertar('estudiantes', e)
    for mat in lista_kardex:
        m = {
            'idKardex': mat[0],
            'control': mat[1],
            'materia': mat[2],
            'calificacion': float(mat[3])
        }
        obj_PyMongo.insertar('kardex', m)
    for usr in lista_usuarios:
        u = {
            'idUsuario': usr[0],
            'control': usr[1],
            'clave': usr[2],
            'clave_cifrada': usr[3]
        }
        obj_PyMongo.insertar('usuarios', u)
    obj_PyMongo.desconectar_mongodb()


# cargar_estudiantes()


def insertar_estudiante():
    obj_PyMongo = PyMongo(varmongo)
    print(" == INSERTAR ESTUDIANTES ==")
    ctrl = input("Dame el numero de control: ")
    nombre = input("Dame el nombre del estudiante: ")
    clave = input("Dame la clave de acceso: ")
    obj_usuario = Password(longitud=len(clave), contrasena=clave)
    json_estudiante = {'control': ctrl, 'nombre': nombre}  # f"INSERT INTO estudiantes values('{ctrl}','{nombre}');"
    json_usuario = {'idUsuario': 100, 'control': ctrl, 'clave': clave,
                    'clave_cifrada': obj_usuario.contrasena_cifrada.decode()}  # f'INSERT INTO usuarios(control,clave,clave_cifrada) values("{ctrl}","{clave}","{obj_usuario.contrasena_cifrada.decode()}");'
    # print(sql_usuario)
    obj_PyMongo.conectar_mongodb()
    obj_PyMongo.insertar('estudiantes', json_estudiante)
    obj_PyMongo.insertar('usuarios', json_usuario)
    obj_PyMongo.desconectar_mongodb()
    print("Datos insertados correctamente")


def actualizar_calificacion():
    obj_PyMongo = PyMongo(varmongo)
    print(" == ACTUALIZAR PROMEDIO ==")
    ctrl = input("Dame el numero de control: ")
    materia = input("Dame la materia a actualizar: ")
    filtro_buscar_materia = {'control': ctrl, 'maeteria': materia}
    obj_PyMongo.conectar_mongodb()
    respuesta = obj_PyMongo.consulta_mongodb('kardex', filtro_buscar_materia)
    for reg in respuesta:
        print(reg)
    if respuesta:
        promedio = float(input("Dame el nuevo promedio: "))
        sql_actualiza_prom = {"$set": {"calificacion": promedio}}
        # f"UPDATE kardex set calificacion={promedio} " \
        # f"WHERE control='{ctrl}' and materia='{materia.strip()}';"
        resp = obj_PyMongo.actualizar('kardex', filtro_buscar_materia, sql_actualiza_prom)
        if resp['status']:
            print("Promedia ha sido actualizado")
        else:
            print("Ocurrio un eror al actualizar")
    else:
        print(f"El estudiante con numero de control {ctrl} o la materia: {materia} NO EXISTE")
    obj_PyMongo.desconectar_mongodb()


def consultar_materias():
    obj_PyMongo = PyMongo(varmongo)
    print("********** CONSULTAR MATERIAS POR ESTUDIANTE **********")
    ctrl = input("Dame el numero de control: ")
    filtro = {'control': ctrl}
    atributos_estudiante = {"_id": 0, "nombre": 1}
    atrbutos_kardex = {"_id": 0, "materia": 1, "calificacion": 1}
    # sql_materias ="SELECT E.nombre, K.materia, K.calificacion " \
    #               "FROM estudiantes E, kardex K " \
    #               f"WHERE E.control = K.control and E.control='{ctrl}';"
    # print(sql_materias)
    obj_PyMongo.conectar_mongodb()
    respuesta1 = obj_PyMongo.consulta_mongodb('estudiantes', filtro, atributos_estudiante)
    respuesta2 = obj_PyMongo.consulta_mongodb('kardex', filtro, atrbutos_kardex)
    obj_PyMongo.desconectar_mongodb()
    # print("respuesta1", respuesta1)
    # print("respuesta2", respuesta2)
    if respuesta1["status"] and respuesta2["status"]:
        print("Estudiante: ", respuesta1["resultado"][0]["nombre"])
        for mat in respuesta2["resultado"]:
            print("Materia: ", mat[1], "Calificacion:", mat[2])
    else:
        print(f"El resultado con el numero de control: {ctrl} No existe")
    # resp = obj_MySQL.consulta_sql(sql_materias)
    # if resp:
    #     print("Estudiante: ", resp[0][0])
    #     for mat in resp:
    #         print("Materia: ",mat[1], " Calificación: ", mat[2])
    # else:
    #     print (f"El estudiante con Número de control: {ctrl} NO existe")


def consulta_general():
    obj_PyMongo = PyMongo(varmongo)
    print("********** CONSULTA GENERAL **********")
    atributos_estudiante = {"_id": 0, "nombre": 1}
    atrbutos_kardex = {"_id": 0, "materia": 1, "calificacion": 2}
    atributos_usuario = {"ctrl": 0, "clave": 1, "clave_cifrada": 2}
    # sql_materias ="SELECT E.nombre, K.materia, K.calificacion " \
    #               "FROM estudiantes E, kardex K " \
    #               f"WHERE E.control = K.control and E.control='{ctrl}';"
    # print(sql_materias)
    obj_PyMongo.conectar_mongodb()
    respuesta1 = obj_PyMongo.consulta_mongodb('estudiantes', atributos_estudiante)
    respuesta2 = obj_PyMongo.consulta_mongodb('kardex', atrbutos_kardex)
    respuesta3 = obj_PyMongo.consulta_mongodb('usuarios', atributos_usuario)
    obj_PyMongo.desconectar_mongodb()
    # print("respuesta1", respuesta1)
    # print("respuesta2", respuesta2)
    for est in respuesta1["resultado"]:
        print("Control:", est[0], "Nombre:", est[1])
        for mat in respuesta2["resultado"]:
            print("Materia: ", mat[1], "Calificacion:", mat[2])
            for usr in respuesta3["resultado"]:
                print("control:", usr[1], "clave:", usr[2], "Clave_Cifrada", usr[3])
# resp = obj_MySQL.consulta_sql(sql_materias)
# if resp:
#     print("Estudiante: ", resp[0][0])
#     for mat in resp:
#         print("Materia: ",mat[1], " Calificación: ", mat[2])
# else:
#     print (f"El estudiante con Número de control: {ctrl} NO existe")
def eliminar_estudiante():
    obj_PyMongo = PyMongo(varmongo)
    print(" == ELIMINAR ESTUDIANTE ==")
    ctrl = input("Dame el numero de control a borrar: ")
    filtro_buscar_estudiante = {'control': ctrl}
    obj_PyMongo.conectar_mongodb()
    respuesta = obj_PyMongo.consulta_mongodb('estudiantes', filtro_buscar_estudiante)
    for reg in respuesta:
        print(reg)
    if respuesta:
        sql_borrar_estudiante = {"$delete_one": {"Numero de control": ctrl}}
        # f"UPDATE kardex set calificacion={promedio} " \
        # f"WHERE control='{ctrl}' and materia='{materia.strip()}';"
        resp = obj_PyMongo.delete_one('estudiantes', filtro_buscar_estudiante)
        if resp['status']:
            print("el estudiante a sido borrado")
        else:
            print("Ocurrio un error al borrar")
    else:
        print(f"El estudiante con numero de control {ctrl} NO EXISTE")
    obj_PyMongo.desconectar_mongodb()



def menu():
    while (True):
        print("***************  Menú Principal  ****************************")
        print("1. Insertar estudiante")
        print("2. Actualizar calificación")
        print("3. Consultar materias por estudiante")
        print("4. Consulta general de estudiantes")
        print('5. Eliminar un estudiante')
        print("6. Salir")
        print("Selecciona una opción: ")
        try:
            opcion = int(input(""))
        except Exception as error:
            print("Error", error)
            break
        else:
            if opcion == 1:
                insertar_estudiante()
            elif opcion == 2:
                actualizar_calificacion()
            elif opcion == 3:
                consultar_materias()
            elif opcion == 4:
                consulta_general()
            elif opcion == 5:
                eliminar_estudiante()
            elif opcion == 6:
                break
            else:
                print("Opción incorrecta")


menu()
