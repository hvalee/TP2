package sys.mapreduce.distributed.soap.v1;

import static sys.mapreduce.distributed.soap.v1.MapReduceWorker.NAME;
import static sys.mapreduce.distributed.soap.v1.MapReduceWorker.NAMESPACE;

import java.net.MalformedURLException;
import java.net.URI;

import javax.xml.namespace.QName;
import javax.xml.ws.Service;

import sys.mapreduce.distributed.soap.SoapWorkerFactory;


public class MapReduceWorkerClientFactory implements SoapWorkerFactory<MapReduceWorker>{

	private static final QName QNAME = new QName(NAMESPACE, NAME);

	@Override
	public MapReduceWorker createWorker(URI workerURI) {
		try {
			Service service;
			service = Service.create(workerURI.toURL(), QNAME);
			return service.getPort(MapReduceWorker.class);
		} catch (MalformedURLException e) {
			e.printStackTrace();
			throw new RuntimeException(e.getMessage());
		}
	}
}
