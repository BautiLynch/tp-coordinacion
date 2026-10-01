# Informe de Trabajo Practico Coordinacion

Para extender la arquitectura provista por la catedra y cubrir las limitaciones que se especifican del mismo, se agrego la estructura de middleware MOM elaborado en el TP anterior para hacer uso de las estructuras de Queues y Exchanges que ya tenemos desarrollada. Esta fue la base para todas las mejoras que se realizaron en este TP. Se agregaron funcionalidades al middleware, mecanismos de identificacion, particionamiento y coordinacion necesarios para procesar multiples clientes y poder escalar los Sum y Aggregation.

## Identificacion de clientes

El primer cambio a la arquitectura provista fue agregar la estructura necesaria para poder procesar varios clientes al mismo tiempo. Esto se logro cambiando levemente el mensaje que se envia desde el gateway al resto de los controladores, empezando por los SUM. Todo mensaje saliente del gateway se le agrega un UUID a cada cliente desde el `MessageHandler`, lo cual se asegura que el mismo cliente tenga el mismo UUID. Este mismo dato se agrega a todos los mensajes que se pasan entre controladores.

Cada controlador entonces guarda en diccionarios indexados por el identificador de cada cliente para almacenar las diferentes frutas que reciben de cada uno. Asi, el Join incluye tambien este valor en el mensaje de respuesta al gateway, para que se pueda enviar la respuesta al cliente especifico.

## Coordinacion entre Sums
Las instancias de Sum consumen de una misma work queue. RabbitMQ distribuye los mensajes entre ellas, permitiendo repartir el procesamiento cuando aumenta el volumen de entrada. Las colas utilizan `prefetch_count=1`, como especifica el tutorial de RabbitMQ para work queues, por lo que cada instancia recibe un unico mensaje a procesar hasta que sea acknowledged.

El EOF enviado por el Gateway tambien es consumido por una unica instancia de Sum, al igual que cada dato individual de un cliente. Como todas las instancias pueden tener datos procesando del cliente, la instancia lo publica por un exchange de control al resto (incluido a el mismo), ya que cada Sum tiene una queue asociada a la misma routing key. 

Cada Sum tiene dos threads, el principal que consume los datos del Gateway y suma los valores de frutas por cliente, y el secundario que escucha a los EOF del exchange de control. El envio y recepcion de los EOF se usan sobre el mismo exchange, pero debido a la implementacion del Middleware que creamos en el TP anterior, cada exchange crea una `BlockingConnection` diferente. Esto implico la creacion de dos conexiones para el mismo exchange para enviar y otro para recibir datos, ya que no se pueden reutilizar estas conexiones desde distintos thread sin usar ciertos auxiliares que lo permiten en algunos casos.

Para coordinar estos threads, se utiliza una `condvar`, que indica que cliente se esta procesando en el thread principal. Si el thread de control recibe un EOF del mismo cliente, debe esperar a que finalice el mensaje en curso. Si algun Sum recibio el EOF, eso significa que el resto de los mensajes de datos ya fueron distribuidos entre los Sum. Como la implementacion realizada de las work queues sigue lo definido en la documentacion de RabbitMQ, es decir que utilizan el `prefetch_count=1`, tenemos asegurado que el mensaje que estamos procesando sera como mucho el ultimo de ese cliente para ese Sum, asi evitando race conditions. Ademas, se usa un `lock` para asegurar la consistencia de escritura/lectura/borrado de los datos de los clientes entre los threads.

## Coordinacion entre Sum y Aggregation

Los mensajes entre Sum y Aggregation fueron particionados mediante sharding usando un algoritmo de hashing por cada par `<cliente><fruta>`. Esto permitio el particionamiento de los datos para dividir la carga de los aggregations de una forma deterministica entre procesos y permitir distribuir los datos entre las replicas. Ademas, asegura que los parciales de una misma fruta para un mismo cliente lleguen siempre al mismo Aggregator. El algoritmo que se uso fue `adler32`, un algoritmo deterministico y rapido, y no es un algoritmo criptografico, para que no afecte al rendimiento del sistema.

Todas las instancias de Sum publican sobre una unica conexion a un unico exchange, a diferencia de la separacion que se hacia en el esqueleto original donde se usaba una conexion para cada Aggregator. Estos ultimos tienen una routing key propia asociada a la queue de donde consumen. Esta decision obligo a hacer un cambio en el middleware, donde se agrego el metodo `send_rk` que permite elegir una routing key en particular para enviar cada mensaje por el mismo exchange.

Una vez que se transmiten todos los mensajes de datos, cada Sum envia un mensaje EOF a cada Aggregator.


## Coordinacion entre Aggregation y Join

Cada Aggregator mantiene por separado los datos y la cantidad de EOF recibidos por cliente. Al recibir los EOF individuales de cada Sum, espera hasta alcanzar el total de EOF para ese cliente de la cantidad total de Sum. Luego calcula el top parcial con sus datos y lo envia al Join. 

El Join mantiene el top del cliente con los datos que recibio hasta el momento del tamaño especificado. Al llegar un nuevo mensaje, agrega el nuevo top parcial, lo ordena y vuelve a recortar la lista al tamaño especificado. Cuando la cantidad de mensajes que llegaron de ese cliente es igual al total de Aggregators, usa el ultimo resultado calculado para enviar al gateway y devolver al cliente.


## Graceful shutdown

Tanto Sum, Aggregation y Join manejan SIGTERM mediante un evento de finalizacion. Al recibir la señal, dejan de consumir mensajes y cierran los canales y conexiones.
En Sum, tambien se despierta a la condvar y se toma en cuenta el caso de finalizacion en la espera y toma de condvar en el thread de control. Ademas se detiene el consumo del thread de control y se le realiza un join antes de liberar los recursos. Para poder utilizar el `stop_consuming` desde el thread principal para poder desbloquear al thread de control, se tuvo que modificar el metodo para usar `add_callback_threadsafe` y asegurar el uso entre varios threads.

## Escalabilidad

El sistema permite procesar multiples clientes concurrentemente, gracias a los identificadores unicos utilizados para los mensajes y estados internos. Ademas tambien se escala frente a grandes volumenes de datos ya que los Sum y Aggregators se reparten los mensajes, lo que permite agregar replicas de ambos. La cantidad de replicas de los Sums y Aggregators tambien pueden aumentar gracias a las garantias que ya se describieron sobre el protocolo implementado.
