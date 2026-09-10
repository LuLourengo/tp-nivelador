# Informe Técnico: TP0

## Protocolo de Comunicación
Para la transferencia de datos entre el cliente y el servidor se diseñé un protocolo binario orientado a mensajes sobre TCP, garantizando un flujo eficiente y sin pérdida de datos. 
* **Estructura del Mensaje:** Cada mensaje enviado por la red tiene un encabezado fijo de 5 bytes en orden de bytes big-endian, compuesto por un `uint32` que indica la longitud exacta del payload seguido de un `1 byte` que especifica el tipo de mensaje.
* **Tipos de Mensajes:** Definí 4 tipos de mensaje: `MSG_BET` (envío de lotes de apuestas), `MSG_END` (notificación de cierre por parte de la agencia), `MSG_WINNERS` (solicitud y recepción de ganadores) y `MSG_ACK` (confirmación de recepción de lotes).

## Sincronización y Concurrencia
El servidor lo diseñé con un modelo multihilo (`threading` en Python) para atender múltiples conexiones de clientes en paralelo de manera segura:
* **Exclusión Mutua:** Usé locks (`threading.Lock`) para proteger el acceso concurrente al almacenamiento compartido de las apuestas (`bets.csv`), evitando condiciones de carrera al registrar lotes simultáneos de distintas agencias.
* **Control de Quórum y Barrera:** Para la sincronización de la etapa de cierre,use un mecanismo de variables de condición (`threading.Condition`). El servidor contabiliza las agencias finalizadas mediante un conjunto, si el quórum mínimo definido (`AGENCY_QUORUM_MIN`) no se alcanzó, los hilos de los clientes se bloquean y esperan de forma ordenada hasta que todas las agencias requeridas completen su carga, momento en el cual se destraban de forma conjunta para hacer el cálculo y filtrado de los ganadores.

## Uso de biblioteca no autorizada

Durante la etapa de integración y pruebas automatizadas, tuve problemas con el test de memoria, ya que algunas veces pasaba la prueba con OK y otras no. Vi que en el campus a algunos compañeros les pasaba lo mismo y probé lo sugerido de ir cambiando el número de segundos en el `pooling_await_seconds`, pero aun así siguió fallando. Investigué un poco y encontré que el error podría ser porque el recolector de basura de Go a veces retiene memoria, y que se podía usar `debug` para resolverlo. Sé que no estaba permitido usar este tipo de bibliotecas/paquetes, pero para poder continuar con el TP usé `debug` (`debug.SetMemoryLimit` y `debug.FreeOSMemory()`) para acotar la memoria y forzar la limpieza al terminar. Con eso pude destrabar las pruebas y seguir avanzando con el resto del trabajo. Una vez terminado el resto de los ítems, volví a revisar eso, pero no lo pude resolver de otra manera.
