package sys.mapreduce.distributed.soap;

import java.net.URI;

public interface SoapWorkerFactory<W> {
	W createWorker( URI uri );
}
