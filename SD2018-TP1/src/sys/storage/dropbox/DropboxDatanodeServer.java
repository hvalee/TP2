package sys.storage.dropbox;

import com.github.scribejava.core.builder.ServiceBuilder;
import com.github.scribejava.core.model.OAuth2AccessToken;
import com.github.scribejava.core.model.OAuthRequest;
import com.github.scribejava.core.model.Response;
import com.github.scribejava.core.model.Verb;
import com.github.scribejava.core.oauth.OAuth20Service;
import org.glassfish.jersey.jdkhttp.JdkHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.pac4j.scribe.builder.api.DropboxApi20;
import sys.storage.dropbox.msgs.*;
import utils.JSON;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import java.net.URI;
import java.util.Scanner;



public class DropboxDatanodeServer {
    private static final String apiKey = "n77x0i4pkxqs8u8";
    private static final String apiSecret = "dy0zqbsfdh5bhue";
    private static final String CREATE = "https://api.dropboxapi.com/2/files/upload";
    private static final String GET = "https://api.dropboxapi.com/2/files/download";
    private static final String LIST_FOLDER_V2_URL = "https://api.dropboxapi.com/2/files/list_folder";
    private static final String LIST_FOLDER_CONTINUE_V2_URL = "https://api.dropboxapi.com/2/files/list_folder/continue";
    private static final String CREATE_FOLDER_V2_URL = "https://api.dropboxapi.com/2/files/create_folder_v2";
    private static final String DELETE = "https://api.dropboxapi.com/2/file_requests/delete_v2";

    protected static final String JSON_CONTENT_TYPE = "application/json; charset=utf-8";
    protected static final String OCTET_CONTENT_TYPE = "application/octet-stream; charset=utf-8";
    protected static final String ADD = "add";

    protected OAuth20Service service;
    protected OAuth2AccessToken accessToken;

    protected DropboxDatanodeServer() {
        try {
            OAuthCallbackServlet.start();
            service = new ServiceBuilder().apiKey(apiKey).apiSecret(apiSecret)
                    .callback(OAuthCallbackServlet.CALLBACK_URI)
                    .build(DropboxApi20.INSTANCE);

            String authorizationURL = service.getAuthorizationUrl();
            System.out.println("Open the following URL in a browser:\n" + authorizationURL);

            String authorizationID;
            try(Scanner sc = new Scanner( System.in ) ) {
                System.out.print("\n\nAuthorization code: ");
                authorizationID = sc.nextLine();
            }
            accessToken = service.getAccessToken( authorizationID );
        } catch( Exception x) {
            x.printStackTrace();
            System.exit(0);
        }
    }

    @Path("/")
    public static class OAuthCallbackServlet {
        public static final String CALLBACK_URI = "https://localhost:5555/";


        @GET
        public String callback(@QueryParam("code") String code ) {
            return String.format("<html>Authorization-Code: %s</html>", code);
        }


        public static void start() {
            ResourceConfig config = new ResourceConfig();
            config.register( new OAuthCallbackServlet() );
            JdkHttpServerFactory.createHttpServer( URI.create(CALLBACK_URI), config);
        }
    }

    public void createFile(String path) throws Exception{
        OAuthRequest createFile = new OAuthRequest(Verb.POST, CREATE);
        createFile.addHeader("Content-Type", OCTET_CONTENT_TYPE);
        createFile.addHeader("Dropbox-API-arg", JSON.encode(new CreateFileArgs(path,ADD,false, false)));
        String title;

        /*try(Scanner sc = new Scanner( System.in ) ) {
            System.out.print("\n\nTitle for File: ");
            title = sc.nextLine();
        }*/

        createFile.setPayload( JSON.encode(new CreateFileArgs(path,ADD,false, false)));

        service.signRequest(accessToken, createFile);
        Response r = service.execute(createFile);

        if (r.getCode() == 409) {
            System.err.println("Dropbox file already exists");
        } else if (r.getCode() == 200) {
            System.err.println("Dropbox file was created with success");
            return;
        } else {
            System.err.println("Unexpected error HTTP: " + r.getCode());
        }


    }

    public void getFile(String path) throws Exception{
        OAuthRequest getFile = new OAuthRequest(Verb.POST, GET);
        getFile.addHeader("Content-Type", JSON_CONTENT_TYPE);
        getFile.addHeader("Dropbox-API-arg", JSON.encode(new GetFileArgs(path)));

        getFile.setPayload( JSON.encode(new GetFileArgs(path)));

        service.signRequest(accessToken, getFile);
        //Response r = service.execute(getFile);


    }

    public void deleteFile(String path) throws Exception{
        OAuthRequest deleteFile = new OAuthRequest(Verb.POST, DELETE);
        deleteFile.addHeader("Content-Type", JSON_CONTENT_TYPE);

        deleteFile.setPayload( JSON.encode(new DeleteFilesArgs(path)));

        service.signRequest(accessToken, deleteFile);
        Response r = service.execute(deleteFile);



    }


    public void createDirectory(String path) throws Exception{
        OAuthRequest createFolder = new OAuthRequest(Verb.POST, CREATE_FOLDER_V2_URL);
        createFolder.addHeader("Content-Type", JSON_CONTENT_TYPE);
        createFolder.setPayload( JSON.encode(new CreateFolderV2Args(path, false	)));

        for(;;) {
            service.signRequest(accessToken, createFolder);
            Response r = service.execute(createFolder);
            if (r.getCode() != 200)
                throw new RuntimeException("Failed: " + r.getMessage() );

            CreateFolderV2Return result = JSON.decode( r.getBody(), CreateFolderV2Return.class);
            result.getEntries().forEach( System.out::println );

        }

    }

    public void listDirectory(String path) throws Exception{
        OAuthRequest listFolder = new OAuthRequest(Verb.POST, LIST_FOLDER_V2_URL);
        listFolder.addHeader("Content-Type", JSON_CONTENT_TYPE);
        listFolder.setPayload( JSON.encode(new ListFolderV2Args(path, true)));

        for(;;) {
            service.signRequest(accessToken, listFolder);
            Response r = service.execute(listFolder);
            if (r.getCode() != 200)
                throw new RuntimeException("Failed: " + r.getMessage() );

            ListFolderV2Return result = JSON.decode( r.getBody(), ListFolderV2Return.class);
            result.getEntries().forEach( System.out::println );

            if(result.has_more()) {
                System.err.println("continuing...");
                listFolder = new OAuthRequest(Verb.POST, LIST_FOLDER_CONTINUE_V2_URL);
                listFolder.addHeader("Content-Type", JSON_CONTENT_TYPE);
                listFolder.setPayload( JSON.encode(new ListFolderContinueV2Args( result.getCursor() )));
            } else
                break;
        }
    }

}