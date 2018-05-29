package api.mapreduce;

import javax.jws.WebMethod;
import javax.jws.WebService;
import javax.xml.ws.WebFault;

@WebService
public interface ComputeNode {
		static final String PATH = "/mapreduce";	
		static final String NAME="ComputeService";
		static final String NAMESPACE="http://sd2018";
		static final String INTERFACE="api.mapreduce.ComputeNode";
	
		@WebFault
	    class InvalidArgumentException extends Exception {

	        private static final long serialVersionUID = 1L;

	        public InvalidArgumentException() {
	            super("");
	        }        
	        public InvalidArgumentException(String msg) {
	            super(msg);
	        }
	    }
		
		@WebMethod
		void mapReduce( String jobClassBlob, String inputPrefix , String outputPrefix, int outputPartitionsSize ) throws InvalidArgumentException;
}
