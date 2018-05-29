package sys.mapreduce.distributed.soap.v2;

import javax.jws.WebMethod;
import javax.jws.WebService;

@WebService
public interface MapReduceWorker {
	static final String PATH = "/mapreduce";
	static final String NAME = "MapReduceWorker";
	static final String NAMESPACE = "http://sd2018";
	static final String INTERFACE = "sys.mapreduce.distributed.soap.v2.MapReduceWorker";

	@WebMethod
	void newJob(String jobClassBlob, String inputPrefix, String outputPrefix, String jobId);

	void executeMapPhase( String jobId );
	
	void executeReducePhase( String jobId );
}
