package surfstore;

import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.concurrent.Executors;
import java.util.logging.Logger;
import java.util.TimerTask;

import com.google.protobuf.ByteString;
import com.google.protobuf.ProtocolStringList;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import surfstore.SurfStoreBasic.Empty;
import surfstore.SurfStoreBasic.FileInfo;
import surfstore.SurfStoreBasic.FileInfo.Builder;
import surfstore.SurfStoreBasic.WriteResult;
import surfstore.SurfStoreBasic.WriteResult.Result;
import surfstore.SurfStoreBasic.SimpleAnswer;
import surfstore.SurfStoreBasic.StateMachine;
import surfstore.MetadataStoreGrpc.MetadataStoreBlockingStub;
import surfstore.SurfStoreBasic.Block;

public final class MetadataStore {
    private static final Logger logger = Logger.getLogger(MetadataStore.class.getName());

    protected Server server;
	protected ConfigReader config;
	protected ArrayList<MetadataStoreGrpc.MetadataStoreBlockingStub> connections;
	
    private final ManagedChannel blockChannel;
    private final BlockStoreGrpc.BlockStoreBlockingStub blockStub;

    public MetadataStore(ConfigReader config) {
    	this.config = config;
        this.blockChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getBlockPort())
                .usePlaintext(true).build();
        this.blockStub = BlockStoreGrpc.newBlockingStub(blockChannel);
        connections = new ArrayList<MetadataStoreGrpc.MetadataStoreBlockingStub>();

	}

	private void start(int serverNum, int port, int numThreads) throws IOException {
        server = ServerBuilder.forPort(port)
                .addService(new MetadataStoreImpl(this, serverNum))
                .executor(Executors.newFixedThreadPool(numThreads))
                .build()
                .start();
        logger.info("Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                MetadataStore.this.stop();
                System.err.println("*** server shut down");
            }
        });
    }

    private void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    private static Namespace parseArgs(String[] args) {
        ArgumentParser parser = ArgumentParsers.newFor("MetadataStore").build()
                .description("MetadataStore server for SurfStore");
        parser.addArgument("config_file").type(String.class)
                .help("Path to configuration file");
        parser.addArgument("-n", "--number").type(Integer.class).setDefault(1)
                .help("Set which number this server is");
        parser.addArgument("-t", "--threads").type(Integer.class).setDefault(10)
                .help("Maximum number of concurrent threads");

        Namespace res = null;
        try {
            res = parser.parseArgs(args);
        } catch (ArgumentParserException e){
            parser.handleError(e);
        }
        return res;
    }

    public static void main(String[] args) throws Exception {
        Namespace c_args = parseArgs(args);
        if (c_args == null){
            throw new RuntimeException("Argument parsing failed");
        }
        
        File configf = new File(c_args.getString("config_file"));
        ConfigReader config = new ConfigReader(configf);

        if (c_args.getInt("number") > config.getNumMetadataServers()) {
            throw new RuntimeException(String.format("metadata%d not in config file", c_args.getInt("number")));
        }

        final MetadataStore server = new MetadataStore(config);
        server.start(c_args.getInt("number"), config.getMetadataPort(c_args.getInt("number")), c_args.getInt("threads"));
        server.blockUntilShutdown();
    }

    static class MetadataStoreImpl extends MetadataStoreGrpc.MetadataStoreImplBase 
    {
    	protected Map<String, surfstore.SurfStoreBasic.FileInfo> metaMap;
    	private MetadataStore ms;
    	private ConfigReader config;
    	protected boolean isLeader;
    	protected boolean isCrashed;
    	protected ArrayList<String> stateMachine;
    	protected int serverNum;
    	protected MetadataStoreImpl cur;
    	
    	public MetadataStoreImpl(MetadataStore obj, int serverNum)
    	{
    		super();
    		this.metaMap = new HashMap<String, surfstore.SurfStoreBasic.FileInfo>();
    		stateMachine = new ArrayList<String>();
    		this.ms = obj;
    		this.config = obj.config;
    		this.serverNum = serverNum;
    		this.cur = this;
            Timer timer = new Timer();
            
            if(config.getLeaderNum() == serverNum)
            	isLeader = true;
            for(int i = 1; i <= config.getNumMetadataServers(); i++)
            {
            	ManagedChannel mc = ManagedChannelBuilder.forAddress("127.0.0.1", config.getMetadataPort(i)).usePlaintext(true).build();
    			MetadataStoreGrpc.MetadataStoreBlockingStub metadataStub = MetadataStoreGrpc.newBlockingStub(mc);
    			
    			if(config.getLeaderNum() != i)
    				ms.connections.add(metadataStub);
            }
            if(isLeader)
            {
            	timer.schedule(new TimerTask() {
            		@Override
            		public void run() {   	
            			while(!cur.updateAllFollowerLogs());
            			try {
            				StateMachine.Builder toSend = StateMachine.newBuilder();
                    		toSend.putAllCommitedMap(metaMap);
                    		StateMachine commitedMap = toSend.build();
            				for(MetadataStoreBlockingStub stub : ms.connections)
                			{
            					stub.updateMetaMap(commitedMap);
                			}
            			}
            			catch(Exception E) {}	//this will catch if followers aren't running yet
            		}
            	}, 0, 500);
            }
    	}
    	
        @Override
        public void ping(Empty req, final StreamObserver<Empty> responseObserver) {
            Empty response = Empty.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } 
		public void readFile(surfstore.SurfStoreBasic.FileInfo request,
                io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.FileInfo> responseObserver) 
        {
        	Builder fileExist = FileInfo.newBuilder();
        	
        	if(metaMap.containsKey(request.getFilename()) == false)
        	{
        		fileExist.setFilename(request.getFilename());
        		fileExist.setVersion(0);
        	}
        	else
        	{
        		fileExist.setVersion(metaMap.get(request.getFilename()).getVersion() );
        		fileExist.setFilename(metaMap.get(request.getFilename()).getFilename());
        		fileExist.addAllBlocklist(metaMap.get(request.getFilename()).getBlocklistList());
        	}
        	FileInfo response = fileExist.build();
        	responseObserver.onNext(response);
        	responseObserver.onCompleted();
        }
        
        private static Block stringToBlock(byte[] s)
        {
        	Block.Builder builder = Block.newBuilder();
            String encoded = Base64.getEncoder().encodeToString(s);
        		
        	builder.setHash(encoded);
        	
        	return builder.build();
        }
//        message WriteResult {
//            enum Result {
//                OK = 0;
//                OLD_VERSION = 1;
//                MISSING_BLOCKS = 2;
//                NOT_LEADER = 3;
//            }
//            Result result = 1;
//            int32 current_version = 2;
//            repeated string missing_blocks = 3;
//        }
        // Come back realpath ask later
        public void modifyFile(surfstore.SurfStoreBasic.FileInfo request, io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.WriteResult> responseObserver) 
        {
        	synchronized(this)
			{
	        	WriteResult.Builder result = WriteResult.newBuilder();
	        	Block.Builder builder = Block.newBuilder();
	        	boolean missing = false;
	        	if(!isLeader)
	        	{
	        		result.setResult(Result.NOT_LEADER);
	        		result.setCurrentVersion(metaMap.get(request.getFilename()).getVersion());
	        	}
	        	else if(request.getVersion() == 0 ||  !metaMap.containsKey(request.getFilename()) ) //Creating a new file
	        	{
	        		stateMachine.add("modifyFile");
	        		metaMap.put(request.getFilename(), request);
	        		ArrayList<String> missBlockList = new ArrayList<String>();
	        		for(String s : request.getBlocklistList())
	        		{
	    				if(s == "0")
	    					continue;
	    				builder.setHash(s);
	    				Block block = builder.build();
	    				if(ms.blockStub.hasBlock(block).getAnswer() == false)
	    				{
	    					missBlockList.add(s);
	    					missing = true;
	    				}
	        		}
	               	if(!missing)
	            	{
	            		FileInfo.Builder newFiB = FileInfo.newBuilder();
	        			newFiB.setVersion(1);
	        			result.setCurrentVersion(1);
	            		newFiB.setFilename(request.getFilename());
	            		newFiB.addAllBlocklist(request.getBlocklistList());
	            		FileInfo newFi = newFiB.build();
	            		metaMap.put(request.getFilename(), newFi);
	            		
	            		StateMachine.Builder toSend = StateMachine.newBuilder();
	            		toSend.putAllCommitedMap(metaMap);
	            		StateMachine commitedMap = toSend.build();
	            		
	        			
	        			for(MetadataStoreBlockingStub stub : ms.connections)
	        			{
	        				//Do we need to check if followers commited sucessfull? If so do we need to do anything with that response flag?
	        				stub.updateMetaMap(commitedMap);
	        			}
	            	}
	            	else
	            	{
	            		result.setResult(Result.MISSING_BLOCKS);
	            		result.addAllMissingBlocks(missBlockList);
	            		result.setCurrentVersion(metaMap.get(request.getFilename()).getVersion());
	            	}
	        	}
	        	else //Updating File part
	        	{
	        		surfstore.SurfStoreBasic.FileInfo metaData = metaMap.get(request.getFilename());
	        		FileInfo.Builder newFiB = FileInfo.newBuilder();
	        		if(request.getVersion() != metaData.getVersion() && request.getVersion() != metaData.getVersion()+1)
	        		{
            			result.setCurrentVersion(metaData.getVersion());
	        			result.setResult(Result.OLD_VERSION);
	        		}
	        		else
	        		{
		    			stateMachine.add("modifyFile");
		    			ArrayList<String> blockList = new ArrayList<String>(request.getBlocklistList());
		    			ArrayList<String> missBlockList = new ArrayList<String>();
		    			for(String s : blockList)
		    			{
		    				if(s == "0")
		    					continue;
		    				builder.setHash(s);
		    				Block block = builder.build();
		    				if(ms.blockStub.hasBlock(block).getAnswer() == false)
		    				{
		    					missBlockList.add(s);
		    					missing = true;
		    				}  					
		    			}
		            	if(!missing)
		            	{   
		            		result.setResult(Result.OK);
		            		while(updateAllFollowerLogs() == false);
	
		            		if(request.getVersion() == metaData.getVersion())
		            		{
		            			result.setResult(Result.OK);
		            			result.setCurrentVersion(metaData.getVersion());
		            			newFiB.setVersion(metaData.getVersion());
		            		}
		            		if(request.getVersion() == metaData.getVersion()+1)
		            		{
		            			result.setCurrentVersion(request.getVersion());
		            			newFiB.setVersion(request.getVersion());
		            			newFiB.setFilename(request.getFilename());
			            		newFiB.addAllBlocklist(blockList);
			            		FileInfo newFi = newFiB.build();
			            		
			            		metaMap.put(request.getFilename(), newFi);
			            		StateMachine.Builder toSend = StateMachine.newBuilder();
			            		toSend.putAllCommitedMap(metaMap);
			            		StateMachine commitedMap = toSend.build();
			            		
			        			for(MetadataStoreBlockingStub stub : ms.connections)
			        			{
			        				//Do we need to check if followers commited sucessfull? If so do we need to do anything with that response flag?
			        				stub.updateMetaMap(commitedMap);
			        			}
		            		}
		            		/*
		            		else	//this is if the client does not update version number, then we handle it on the metaserver (but piazza says no)
		            		{
		        				result.setCurrentVersion(request.getVersion()+1);
		        				newFiB.setVersion(request.getVersion()+1);
		            		}*/
		            		
		            	}
		            	else
		            	{
		            		result.setResult(Result.MISSING_BLOCKS);
		            		result.addAllMissingBlocks(missBlockList);
		            		result.setCurrentVersion(metaData.getVersion());
		            	}
	        		}
	        	}
	        	WriteResult response = result.build();
	        	responseObserver.onNext(response);
	        	responseObserver.onCompleted();
			}
        }

		public void deleteFile(surfstore.SurfStoreBasic.FileInfo request,
        		io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.WriteResult> responseObserver) 
        {
			synchronized(this)
			{
	        	surfstore.SurfStoreBasic.FileInfo metaData = metaMap.get(request.getFilename());
	        	WriteResult.Builder builder = WriteResult.newBuilder();
	        	if(!isLeader)
	        	{
	        		builder.setResult(Result.NOT_LEADER);
	        	}
	        	else if(request.getVersion() != metaData.getVersion()+1)
	        	{
	        		builder.setResult(Result.OLD_VERSION);
	        	}
	        	else
	        	{
	        		stateMachine.add("deleteFile");
	        		FileInfo.Builder newFiB = FileInfo.newBuilder();
	        		newFiB.setVersion(request.getVersion());
	        		newFiB.setFilename(request.getFilename());
	        		newFiB.addAllBlocklist(Arrays.asList("0"));
	        		FileInfo newFi = newFiB.build();
	        		
	        		while(updateAllFollowerLogs() == false);
    				StateMachine.Builder toSend = StateMachine.newBuilder();
            		toSend.putAllCommitedMap(metaMap);
            		StateMachine commitedMap = toSend.build();
    				for(MetadataStoreBlockingStub stub : ms.connections)
        			{
    					stub.updateMetaMap(commitedMap);
        			}
	        		
	        		metaMap.put(request.getFilename(), newFi);
	        		
	        		builder.setCurrentVersion(newFi.getVersion());
	        		builder.addAllMissingBlocks(Arrays.asList("0"));
	        		builder.setResult(Result.OK);
	        	}
	        	builder.setCurrentVersion(metaData.getVersion());
	        	WriteResult response = builder.build();
	        	responseObserver.onNext(response);
	        	responseObserver.onCompleted();
			}
        }


        public void isLeader(surfstore.SurfStoreBasic.Empty request,
        		io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.SimpleAnswer> responseObserver) 
        {
        	//asyncUnimplementedUnaryCall(METHOD_IS_LEADER, responseObserver);
        	SimpleAnswer.Builder result = SimpleAnswer.newBuilder();
        	result.setAnswer(isLeader);
        	SimpleAnswer response = result.build();
        	responseObserver.onNext(response);
        	responseObserver.onCompleted();
        }


        public void crash(surfstore.SurfStoreBasic.Empty request,
        		io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.Empty> responseObserver) 
        {
        	//asyncUnimplementedUnaryCall(METHOD_CRASH, responseObserver);
        	isCrashed = true;
            Empty response = Empty.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }


        public void restore(surfstore.SurfStoreBasic.Empty request,
        		io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.Empty> responseObserver) 
        {
        	//asyncUnimplementedUnaryCall(METHOD_RESTORE, responseObserver);
        	isCrashed = false;
            Empty response = Empty.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }


        public void isCrashed(surfstore.SurfStoreBasic.Empty request,
        		io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.SimpleAnswer> responseObserver) 
        {
        	//asyncUnimplementedUnaryCall(METHOD_IS_CRASHED, responseObserver);
        	SimpleAnswer.Builder result = SimpleAnswer.newBuilder();
        	result.setAnswer(isCrashed);
        	SimpleAnswer response = result.build();
        	responseObserver.onNext(response);
        	responseObserver.onCompleted();
        }
        public void updateMetaMap(surfstore.SurfStoreBasic.StateMachine request, io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.SimpleAnswer> responseObserver)
        {
        	SimpleAnswer.Builder builder = SimpleAnswer.newBuilder();
        	if(isLeader || isCrashed)
        		builder.setAnswer(false);
        	else
        	{
        		builder.setAnswer(true);
        		metaMap = request.getCommitedMapMap();
        	}
        	SimpleAnswer response = builder.build();
        	responseObserver.onNext(response);
        	responseObserver.onCompleted();
        }
        public void appendLog(surfstore.SurfStoreBasic.StateMachine request, io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.SimpleAnswer> responseObserver)
        {
        	SimpleAnswer.Builder result = SimpleAnswer.newBuilder();
        	if(request.getIndex()-1 > stateMachine.size()-1) //gaps
        		result.setAnswer(false);
        	else
        	{
        		stateMachine.add(request.getStateMachine());
        		result.setAnswer(true);
        	}
        	SimpleAnswer response = result.build();
        	responseObserver.onNext(response);
        	responseObserver.onCompleted();      	
        }
        public boolean updateAllFollowerLogs()
        {
        	int crashed = 0;
        	//System.out.println("updateAllFollowerLogs");
        	for(int i = 1; i <= config.getNumMetadataServers(); i++)
        	{
        		try {
        		if(config.getLeaderNum() == i || stateMachine.size() == 0)
        			continue;
        		}
        		catch(Exception E)
        		{
        			System.out.println("Exception");
        			continue;
        		}
    			ManagedChannel mc = ManagedChannelBuilder.forAddress("127.0.0.1", config.getMetadataPort(i)).usePlaintext(true).build();
    			MetadataStoreGrpc.MetadataStoreBlockingStub metadataStub = MetadataStoreGrpc.newBlockingStub(mc);
    			int offset = 0;
    			StateMachine.Builder smb = StateMachine.newBuilder();
    			smb.setIndex(stateMachine.size()-1+offset);
    			smb.setStateMachine(stateMachine.get(stateMachine.size()-1+offset));
    			StateMachine sm = smb.build();
    			Empty temp = Empty.newBuilder().build();
    			if(metadataStub.isCrashed(temp).getAnswer())
    			{
    				crashed++;
    				continue;
    			}
    			while(metadataStub.appendLog(sm).getAnswer() == false)
    			{
    				offset--;
        			smb.setIndex(stateMachine.size()-1+offset);
        			smb.setStateMachine(stateMachine.get(stateMachine.size()-1+offset));
        			sm = smb.build();
    			}
    			while(offset < 0)
    			{
    				offset++;
        			smb.setIndex(stateMachine.size()-1+offset);
        			smb.setStateMachine(stateMachine.get(stateMachine.size()-1+offset));
        			sm = smb.build();
    				metadataStub.appendLog(sm);
    			}
    			
        	}
        	if(crashed > config.getNumMetadataServers()/2)
        		return false;
        	return true;
        }
        // TODO: Implement the other RPCs!
        
        
    }
}