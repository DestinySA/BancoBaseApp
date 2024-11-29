# BancoBaseApp

## 1. Para los ids nulos ¿Qué sugieres hacer con ellos ?

En mi punto de vista los dejaría como vienen de origen. La razón es porque rellenar o eliminar los null podria afectar los análisis posteriores.

## 2. Considerando las columnas name y company_id ¿Qué inconsistencias notas y como las mitigas?

Al revisar los campos se observa que estan vinculados, en este caso se podrían solventar los typos y los nulos.

## 3. Para el resto de los campos ¿Encuentras valores atípicos y de ser así cómo procedes?

En caso de que no afecten al entendimiento de negocio realizaría limpieza de la información. En casos especiales se prefiere el dato de origen a realizar una limpieza. Se debería de valorar con el equipo de negocio.

## 4. ¿Qué mejoras propondrías a tu proceso ETL para siguientes versiones?

Implementarlo de una manera más automatizada, por ejemplo, utilizando Bash Scripting para enviar los archivos (los scripts y los csv) al contenedor o a un bucket de S3, en lugar de hacerlo manualmente.
