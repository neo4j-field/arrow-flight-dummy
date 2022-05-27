# arrow-flight-dummy

> Hold tight, we're in for nasty weather
>               - Burning Down the House, Talking Heads

## build

```
$ ./gradlew shadowJar
```

## run

Without TLS:

```
$ java -jar ./app/build/libs/app-all.jar grpc://0.0.0.0:8492
```

With TLS:

```
$ java -jar ./app/build/libs/app-all.jar grpc+tls://0.0.0.0:8492 \
	./app/build/libs/dummy.crt ./app/build/libs/dummy.key
```

## questions?

Ping Dave Voutila
