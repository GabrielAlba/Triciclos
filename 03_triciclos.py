# -*- coding: utf-8 -*-
"""

Ejercicio 2 : Datos en múltiples ficheros

Gabriel Alba Serrano

"""

import sys
from pyspark import SparkContext


def myMap(line):
    # Divide la línea en dos nodos separados por ' , '
    nodes = line.split(' , ')
    # Si los nodos son distintos devuelve una tupla que representa la conexión
    # entre los nodos en orden ascendente
    if nodes[0] < nodes[1]:
        return (nodes[0], nodes[1])
    elif nodes[1] < nodes[0]:
        return (nodes[1], nodes[0])
    else:
        # Si los nodos son iguales, no se devuelve nada (None)
        pass

def ady_list(l):
    node = l[0]
    ady = l[1]
    n = len(ady)
    result = []
    for i in range(n):
        a = ady[i]
        # Agrega una tupla que representa la conexión entre el nodo principal y el nodo adyacente
        result.append(((node, a), 'exists'))
        for j in range(i + 1, n):
            # Agrega una tupla que representa la conexión pendiente entre el nodo adyacente y los nodos restantes
            result.append(((a, ady[j]), ('pending', node)))


def main(sc, list_files):
    # Para cada arhcivo
    for file in list_files:
        # Se crea un RDD vacío
        g = sc.emptyRDD()

        # Construye el grafo mapeando y eliminando duplicados
        graph = g.map(myMap).distinct().filter(lambda x: x != None)
        # Obtiene una lista de nodos adyacentes ordenados
        ady = graph.groupByKey().map(lambda x: (x[0], sorted(list(x[1])))).sortByKey()
        # Encuentra las conexiones de triciclos en el grafo
        tric = ady.flatMap(ady_list).groupByKey()
        list_tric = []
        for par, msg in tric.collect():
            list_msg = list(msg)
            if len(list_msg) > 1 and 'exists' in list_msg:
                for elem in list_msg:
                    if elem != 'exists':
                        # Agrega la conexión de triciclo a la lista de triciclos encontrados
                        list_tric.append((elem[1], par[0], par[1]))
        # Imprime la lista de triciclos ordenada del archivo 'file'
        print('The Tricycle List of the file', file, ' is: ', sorted(list_tric))
    
    
if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Uso: python3 {0} <file list>'.format(sys.argv[0]))
    else:
        with SparkContext() as sc:
            sc.setLogLevel('ERROR')
            main(sc, sys.argv[1:])