# Prueba Técnica

Para reproducir esta prueba se ha generado un docker-compose
que levanta un contenedor Postgres y un contenedor Python.
La BBDD se inicializa con el script suministrado en la carpeta bd.
El contenedor python instala el paquete creado y ejecuta el script ETL.

Estos 4 puntos se han desarrollado en el paquete etltools que se encuetra en la carpeta "app"
1. Obtener los datos publicados a través de la API:
https://covidtracking.com/data/api (Historic values for all states).
2. Filtrar los datos por aquellos que cumplen la condición:
totalTestResultsSource = totalTestsViral.
3. Generar el modelo de datos que permita analizar los casos positivos
(positive) y muertes confirmadas (deathConfirmed), a través de la fecha
de incidencia (date) y el estado (state).
4. Llevar este modelo de datos a una base de datos relacional desarrollando
un proceso ETL con Python que automatice esta labor.

Estos 2 puntos se han desarrollado en el script que se encuetra en la carpeta "db".
Se ha utiilizado una BBDD Postgres.
5. Generar una vista, en base de datos, que muestre un listado con los
siguientes datos: estado, primera fecha de datos, última fecha de datos,
número de días de datos, suma de casos positivos, suma de muertes
confirmadas, promedio de positivos por número de días, promedio de
positivos por número de días.
6. Generar un procedimiento almacenado que permita borrar los datos
relacionados a un determinado estado en una fecha concreta. Es decir,
que al ejecutar nombre_proc(estado, fecha), se borren los datos de la
tabla generada para ese estado y esa fecha.

El informe adicional se encuentra en la carpeta "eda"

