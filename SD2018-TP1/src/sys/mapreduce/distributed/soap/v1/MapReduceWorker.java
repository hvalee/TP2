package sys.mapreduce.distributed.soap.v1;

import javax.jws.WebMethod;
import javax.jws.WebService;

@WebService
public interface MapReduceWorker {
	static final String PATH = "/mapreduce";
	static final String NAME = "MapReduceWorker";
	static final String NAMESPACE = "http://sd2018";
	static final String INTERFACE = "sys.mapreduce.distributed.soap.v1.MapReduceWorker";

	@WebMethod
	void newJob(String jobClassBlob, String inputPrefix, String outputPrefix, String jobId);

	@WebMethod
	void doMapTask(String jobId, String inputPrefix);

	@WebMethod
	void commitIntermediaResultsToStorage(String jobId);

	@WebMethod
	void doReduceTask(String jobId, String keyPrefix);

	@WebMethod
	void endJob(String jobId);
}
