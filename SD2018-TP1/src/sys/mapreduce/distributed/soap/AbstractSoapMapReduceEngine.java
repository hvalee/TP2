package sys.mapreduce.distributed.soap;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import discovery.Discover;
import sys.mapreduce.AbstractMapReduceEngine;
import sys.mapreduce.distributed.soap.v1.MapReduceWorker;
import sys.storage.rest.RestBlobStorage;
import utils.Random;
import utils.Sleep;

abstract public class AbstractSoapMapReduceEngine<W> extends AbstractMapReduceEngine {

	final private SoapWorkerFactory<W> factory;
	
	final private List<W> workers = new CopyOnWriteArrayList<>();

	protected AbstractSoapMapReduceEngine( SoapWorkerFactory<W> workerFactory ) {
		super( new RestBlobStorage() );
		this.factory = workerFactory;
		new Thread( this::findWorkers ).start();
	}

	protected List<W> availableWorkers() {
		return new ArrayList<>(workers);
	}
	
	protected W randomWorker(List<W> elems) {
		return elems.get( Random.nextInt( elems.size() ));
	}
	
	private void findWorkers() {
		final int RETRY_PERIOD = 100;
		
		Set<URI> uris = new HashSet<>();
		for(;;) {
			URI worker = Discover.uriOf(MapReduceWorker.NAME);
			if( uris.add( worker ) )
				workers.add( factory.createWorker( worker ));
			Sleep.ms(RETRY_PERIOD);			
		}
	}
}
