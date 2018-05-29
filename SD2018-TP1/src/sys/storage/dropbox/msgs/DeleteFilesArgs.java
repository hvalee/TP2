package sys.storage.dropbox.msgs;

public class DeleteFilesArgs {
	final String path;
	
	public DeleteFilesArgs(String path)
	{
		this.path = path;
	}	
}