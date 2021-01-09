# Importation
from pyspark import SparkContext, SparkConf
import numpy as np
import time
from random import *
from operator import add
from math import * # pour avoir la valeur exacte de pi



#Instantiation
sparkConf = SparkConf().setAppName("Estimation_Pi").setMaster("local")
sc = SparkContext(conf = sparkConf)


#Fonctions
def is_point_inside_unit_circle(l):
    x, y = random(), random()   # simuler un point aleatoire comme coord (x,y)
    return 1 if x*x + y*y < 1 else 0 #retourne 1 si le point est dans le cercle, 0 sinon

def pi_estimator_spark(n):
    count = sc.parallelize(range(0, n)) # cree un RDD de la bonne taille
    n_in = count.map(is_point_inside_unit_circle).reduce(add) # probabilité qu'un point soit dans le cercle 
    PI = (4*n_in)/n # calcul de PI
    return PI

def pi_estimator_numpy(n):
    liste_0_1 = np.zeros(n) # cree un vecteur de la bonne taille
    for i in range(n):
        liste_0_1[i] = is_point_inside_unit_circle(i)
    n_in2 = np.sum(liste_0_1==1)   # probabilité qu'un point soit dans le cercle 
    PI = (4*n_in2)/n  # calcul de PI
    return PI




if __name__== '__main__':

    # On garde que les erreurs à afficher dans l'invite de commande
    sc.setLogLevel("ERROR")

    #n = input('Donner la taille de l echantillon aleatoire : ')
    n = 100000

    tmps_debut = time.clock()
    PI = pi_estimator_spark(n)
    tmps_fin = time.clock()
    print('----------------------------------------------------------------------')
    print('Une estimation de pi est',PI)
    print('Le temps d execution en secondes de l algorithme avec Spark est',tmps_fin-tmps_debut)
    print('----------------------------------------------------------------------')


    tmps_debut = time.clock()
    PI = pi_estimator_numpy(n)
    tmps_fin = time.clock()
    print('----------------------------------------------------------------------')
    print('Une estimation de pi est',PI)
    print('Le temps d execution en secondes de l algorithme avec numpy est',tmps_fin-tmps_debut)
    print('----------------------------------------------------------------------')

    print('----------------------------------------------------------------------')
    print('Pour rappel, la vrai valeur de pi est : ',pi)
    print('----------------------------------------------------------------------')
    

    # Arrete context
    sc.stop()


    
