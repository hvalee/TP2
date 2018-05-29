package sys.mapreduce;

import static utils.Log.Log;

import javax.jws.WebService;
import javax.xml.ws.Endpoint;

import api.mapreduce.ComputeNode;
import sys.mapreduce.centralized.CentralizedMapReduceEngine;
import utils.Props;

@WebService(serviceName = ComputeNode.NAME, targetNamespace = ComputeNode.NAMESPACE, endpointInterface = ComputeNode.INTERFACE)
public class ComputeNodeServer implements ComputeNode {
	
	private static String PROPS_FILENAME = "/props/sd2018-tp1.props";
	private static String MAPREDUCE_ENGINE_PROP = "mapreduce-engine";
	
	private static final int COMPUTENODE_PORT = 6666;
	private static String baseURI = String.format("http://0.0.0.0:%d%s", COMPUTENODE_PORT, ComputeNode.PATH);

	final MapReduceEngine engine;

	protected ComputeNodeServer(MapReduceEngine engine) {
		this.engine = engine;
	}
	
	@Override
	public void mapReduce(String jobClassBlob, String inputPrefix, String outputPrefix, int outputPartitionsSize ) throws InvalidArgumentException {
		try {
			if( jobClassBlob == null || inputPrefix == null || outputPrefix == null || outputPartitionsSize <= 0 )
				throw new InvalidArgumentException("");
			
			System.err.println("Executing:" + jobClassBlob);
			engine.executeJob(jobClassBlob, inputPrefix, outputPrefix, outputPartitionsSize);
			System.err.println("Done:" + jobClassBlob);
			
		} catch (Exception x) {
			x.printStackTrace();
		}
	}
	
	
	public static void main(String[] args ) throws Exception {
		
		Props.parseFile(PROPS_FILENAME);
		String engineClass = Props.get(MAPREDUCE_ENGINE_PROP, CentralizedMapReduceEngine.class.toString());
		Log.fine("MapReduceEngine: " + engineClass);

		MapReduceEngine engine = (MapReduceEngine)Class.forName( engineClass).newInstance(); 
		Endpoint.publish(baseURI, new ComputeNodeServer( engine ) );
	}
}
