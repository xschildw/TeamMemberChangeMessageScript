
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.sagebionetworks.client.SynapseAdminClient;
import org.sagebionetworks.client.SynapseAdminClientImpl;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.repo.model.AccessApproval;
import org.sagebionetworks.repo.model.AccessRequirement;
import org.sagebionetworks.repo.model.FileEntity;
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
    private static final int BATCH_SIZE = 100;
    private static final int SKIP_LINES = 0;
    private static int numberOfRecords;

    public static void main(String[] args) throws IOException {
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
        adminSynapse.setUsername(username);
        adminSynapse.setApiKey(apiKey);

        try {
            //process(adminSynapse, filePath);
        	updateARVersion(adminSynapse, filePath);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

	private static void updateARVersion(SynapseAdminClient adminSynapse, String filePath) throws NumberFormatException, IOException {
		numberOfRecords = 0;
		int lineRead = 0;
        CsvReader reader = new CsvReader(filePath);
        while (reader.readRecord()) {
        	lineRead++;
        	try {
        		Long id = Long.valueOf(reader.get(0));
        		AccessRequirement arBefore = adminSynapse.getAccessRequirement(id);
        		AccessRequirement arAfter = adminSynapse.updateAccessRequirement(arBefore);
        		numberOfRecords++;
        	} catch (SynapseException e) {
        		e.printStackTrace();
        	}
        }
        System.out.println("Finish "+numberOfRecords+"/"+lineRead);
        reader.close();
	}

	private static void process(SynapseAdminClient adminSynapse, String filePath) throws IOException, SynapseException {
        numberOfRecords = 0;
        int lineRead = 0;
		CsvReader reader = new CsvReader(filePath);
        // List<ChangeMessage> list = new ArrayList<ChangeMessage>();
        while (reader.readRecord()) {
        	lineRead++;
        	/*if (lineRead < SKIP_LINES) {
        		continue;
        	}*/
        	try {
        		Integer count = Integer.parseInt(reader.get(0));
	        	String first = reader.get(3);
	        	String list = reader.get(4);
	        	String[] ids = list.split(",");
	        	AccessApproval firstAA = adminSynapse.getAccessApproval(Long.parseLong(first));
	        	List<Long> toDelete = new ArrayList<Long>();
	        	for (String id : ids) {
	        		if (id.equals(first)) {
	        			continue;
	        		}
	        		AccessApproval aa = adminSynapse.getAccessApproval(Long.parseLong(id));
	        		if (aa.getConcreteType().equals(firstAA.getConcreteType())) {
	        			toDelete.add(Long.parseLong(id));
	        		} else {
	        			System.out.println("Found difference: "+first+" and "+id);
	        		}
	        	}
	        	toDelete.remove(first);
	        	if (toDelete.size() < count) {
	        		adminSynapse.deleteAccessApprovals(toDelete);
	        		System.out.println("Deleted "+toDelete.size()+ " aa for requirement & user ("+reader.get(1)+", "+reader.get(2)+")");
	        		numberOfRecords++;
	        	} else {
	        		System.out.println("Ignore line "+numberOfRecords);
	        	}
        	} catch (SynapseException e) {
        		e.printStackTrace();
        	}
        	System.out.println("Finish "+numberOfRecords+" records.");
            // ChangeMessage changeMessage = createPrincipalChangeMessage(reader);
        	// ChangeMessage changeMessage = createJDONodeChangeMessage(reader);

        	// ChangeMessage changeMessage = createCertifiedUserPassingRecordChangeMessage(reader);
        	// System.out.println(String.format(touchThread, reader.get(0)));
        	/*ChangeMessage changeMessage = createUpdateThreadChangeMessage(reader);
            list.add(changeMessage);
            numberOfRecords++;

            if (list.size() == BATCH_SIZE) {
                createOrUpdateChangeMessages(adminSynapse, list);
                list = new ArrayList<ChangeMessage>();
            }*/
        }
        /*if (!list.isEmpty()) {
            createOrUpdateChangeMessages(adminSynapse, list);
        }*/
        reader.close();
    }

	private static ChangeMessage createUpdateThreadChangeMessage(CsvReader reader) throws IOException {
		ChangeMessage changeMessage = new ChangeMessage();
		changeMessage.setTimestamp(new Date());
		changeMessage.setObjectId(reader.get(0));
		changeMessage.setObjectEtag(UUID.randomUUID().toString());
		changeMessage.setObjectType(ObjectType.THREAD);
		changeMessage.setChangeType(ChangeType.UPDATE);
		return changeMessage;
	}

	private static ChangeMessage createCertifiedUserPassingRecordChangeMessage(CsvReader reader) throws IOException {
		ChangeMessage changeMessage = new ChangeMessage();
		changeMessage.setTimestamp(new Date());
		changeMessage.setObjectId(reader.get(0));
		changeMessage.setObjectEtag("etag");
		changeMessage.setObjectType(ObjectType.CERTIFIED_USER_PASSING_RECORD);
		changeMessage.setChangeType(ChangeType.UPDATE);
		return changeMessage;
	}

	private static ChangeMessage createJDONodeChangeMessage(CsvReader reader) throws IOException {
		ChangeMessage changeMessage = new ChangeMessage();
		changeMessage.setTimestamp(new Date());
		changeMessage.setObjectId(reader.get(0));
		changeMessage.setObjectEtag("etag");
		changeMessage.setObjectType(ObjectType.ENTITY);
		changeMessage.setChangeType(ChangeType.UPDATE);
		return changeMessage;
	}

	private static ChangeMessage createPrincipalChangeMessage(CsvReader reader) throws IOException {
		ChangeMessage changeMessage = new ChangeMessage();
		changeMessage.setTimestamp(new Date());
		changeMessage.setObjectId(reader.get(0));
		changeMessage.setObjectEtag("etag");
		changeMessage.setObjectType(ObjectType.PRINCIPAL);
		changeMessage.setChangeType(ChangeType.UPDATE);
		return changeMessage;
	}

    private static void createOrUpdateChangeMessages(
            SynapseAdminClient adminSynapse, List<ChangeMessage> list)
            throws SynapseException {
        ChangeMessages batch = new ChangeMessages();
        batch.setList(list);
        adminSynapse.createOrUpdateChangeMessages(batch);
        System.out.println("Created or updated "+numberOfRecords+" change messages.");
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
