package sys.mapreduce;

public interface MapReduceEngine {
	
	void executeJob( String jobClassBlob, String inputPrefix , String outputPrefix, int outputPartitionsSize );
	
}
