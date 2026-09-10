# Informe Técnico: TP0

## Protocolo de Comunicación
Para la transferencia de datos entre el cliente y el servidor se diseñé un protocolo binario orientado a mensajes sobre TCP, garantizando un flujo eficiente y sin pérdida de datos. 
* **Estructura del Mensaje:** Cada mensaje enviado por la red tiene un encabezado fijo de 5 bytes en orden de bytes big-endian, compuesto por un `uint32` que indica la longitud exacta del payload seguido de un `1 byte` que especifica el tipo de mensaje.
* **Tipos de Mensajes:** Definí 4 tipos de mensaje: `MSG_BET` (envío de lotes de apuestas), `MSG_END` (notificación de cierre por parte de la agencia), `MSG_WINNERS` (solicitud y recepción de ganadores) y `MSG_ACK` (confirmación de recepción de lotes).

## Sincronización y Concurrencia
El servidor lo diseñé con un modelo multihilo (`threading` en Python) para atender múltiples conexiones de clientes en paralelo de manera segura:
* **Exclusión Mutua:** Usé locks (`threading.Lock`) para proteger el acceso concurrente al almacenamiento compartido de las apuestas (`bets.csv`), evitando condiciones de carrera al registrar lotes simultáneos de distintas agencias.
* **Control de Quórum y Barrera:** Para la sincronización de la etapa de cierre,use un mecanismo de variables de condición (`threading.Condition`). El servidor contabiliza las agencias finalizadas mediante un conjunto, si el quórum mínimo definido (`AGENCY_QUORUM_MIN`) no se alcanzó, los hilos de los clientes se bloquean y esperan de forma ordenada hasta que todas las agencias requeridas completen su carga, momento en el cual se destraban de forma conjunta para hacer el cálculo y filtrado de los ganadores.

## Incumplimiento de test
Durante la etapa de integración y pruebas automatizadas, tuve problemas con el test de memoria, ya que algunas veces pasaba la prueba con OK y otras no. Sé que no pasa el test de memoria debido al  recolector de basura (*Garbage Collector*) de Go.

Vi que en el campus a algunos compañeros les pasaba lo mismo y probé lo sugerido de ir cambiando el número de segundos en el `pooling_await_seconds`, pero aun así siguió fallando. Investigué un poco y encontré que se podía usar el paquete `debug` para resolverlo. Para poder continuar con el TP, ver el resto de los test y evaluar  las demás funcionalidades, usé temporalmente `debug` (`debug.SetMemoryLimit` y `debug.FreeOSMemory()`) para acotar la memoria y forzar la limpieza, logrando que pase esa prueba. Una vez terminado el resto de los ítems, volví a revisar esto, pero no logre una solucion definitiva sin incumplir con los criterios de correccion, aunque se que el no pasaje de tests es desaprobacion.
