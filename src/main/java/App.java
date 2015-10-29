

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.sagebionetworks.client.SynapseAdminClient;
import org.sagebionetworks.client.SynapseAdminClientImpl;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.repo.model.ObjectType;
import org.sagebionetworks.repo.model.message.ChangeMessage;
import org.sagebionetworks.repo.model.message.ChangeMessages;
import org.sagebionetworks.repo.model.message.ChangeType;

import com.csvreader.CsvReader;

public class App {

    private static SynapseAdminClient adminSynapse;
    private final static String LOCAL_AUTH = "http://localhost:8080/services-repository-develop-SNAPSHOT/auth/v1";
    private final static String LOCAL_REPO = "http://localhost:8080/services-repository-develop-SNAPSHOT/repo/v1";
    private final static String LOCAL_FILE = "http://localhost:8080/services-repository-develop-SNAPSHOT/file/v1";
    private final static String STAGING_AUTH = "https://auth-staging.prod.sagebase.org/auth/v1";
    private final static String STAGING_REPO = "https://repo-staging.prod.sagebase.org/repo/v1";
    private final static String STAGING_FILE = "https://file-staging.prod.sagebase.org/file/v1";
    private static final int BATCH_SIZE = 1000;

    public static void main(String[] args) {
        if (args.length != 4) printUsage();
        String stack = args[0];
        String username = args[1];
        String apiKey = args[2];
        String filePath = args[3];

        adminSynapse = new SynapseAdminClientImpl();
        if (stack == null) {
            printUsage();
        }
        if (!stack.equals("prod")) {
            setEndPoint(adminSynapse, stack);
        }
        adminSynapse.setUserName(username);
        adminSynapse.setApiKey(apiKey);

        try {
            process(adminSynapse, filePath);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void process(SynapseAdminClient adminSynapse, String filePath) throws IOException, SynapseException {
        CsvReader reader = new CsvReader(filePath);
        List<ChangeMessage> list = new ArrayList<ChangeMessage>();
        while (reader.readRecord()) {
            ChangeMessage changeMessage = new ChangeMessage();
            changeMessage.setTimestamp(new Date());
            changeMessage.setParentId(reader.get(0));
            changeMessage.setObjectId(reader.get(1));
            changeMessage.setObjectType(ObjectType.TEAM_MEMBER);
            changeMessage.setObjectEtag("etag");
            changeMessage.setChangeType(ChangeType.UPDATE);
            //System.out.println(changeMessage.toString());
            list.add(changeMessage);

            if (list.size() == BATCH_SIZE) {
                createOrUpdateChangeMessages(adminSynapse, list);
                list = new ArrayList<ChangeMessage>();
            }
        }
        if (!list.isEmpty()) {
            createOrUpdateChangeMessages(adminSynapse, list);
        }
        reader.close();
    }

    private static void createOrUpdateChangeMessages(
            SynapseAdminClient adminSynapse, List<ChangeMessage> list)
            throws SynapseException {
        ChangeMessages batch = new ChangeMessages();
        batch.setList(list);
        adminSynapse.createOrUpdateChangeMessages(batch);
        System.out.println("Created or updated "+list.size()+" team member change messages.");
    }

    private static void printUsage() {
        System.out.println("Usage: ");
        System.out.println("<prod/local/staging> <synapseUsername> <apiKey> <filePath>");
        System.exit(0);
    }

    private static void setEndPoint(SynapseAdminClient adminSynapse, String stack) {
        if (stack.equals("staging")) {
            adminSynapse.setAuthEndpoint(STAGING_AUTH);
            adminSynapse.setRepositoryEndpoint(STAGING_REPO);
            adminSynapse.setFileEndpoint(STAGING_FILE);
        } else if (stack.equals("local")){
            adminSynapse.setAuthEndpoint(LOCAL_AUTH);
            adminSynapse.setRepositoryEndpoint(LOCAL_REPO);
            adminSynapse.setFileEndpoint(LOCAL_FILE);
        } else {
            printUsage();
        }
    }
}
