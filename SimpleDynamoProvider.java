package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.NetworkOnMainThreadException;
import android.telephony.TelephonyManager;
import android.util.Log;

import org.w3c.dom.Node;

public class SimpleDynamoProvider extends ContentProvider {

    static final String TAG = SimpleDynamoProvider.class.getSimpleName();
    static final int SERVER_PORT = 10000;
    private TreeMap<String, String> nodes = new TreeMap<String, String>();
    private HashMap<String,String> ports = new HashMap<String,String>();
    private HashMap<String,String> buffer = new HashMap<String,String>();
    private HashMap<String,String> wbBuffer = new HashMap<String,String>();
    static String myPort = "";
    static String myHash = "";
    private String[] columnNames = {"key", "value"};
    private boolean isNodeDown = false;
    private String nodeDown = "";
    private String succ1 = "";
    private String succ2 = "";

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
        String[] files = getContext().fileList();
        for (String f :
                files) {
            try {
                File file = new File(getContext().getFilesDir(), f);
                file.delete();
                Log.e(TAG,f+" deleted");
            } catch (NullPointerException e) {
                Log.e(TAG,"File already deleted");
            }
            catch (Exception e) {
                Log.e(TAG,"File delete failed");
            }
        }
        Log.v("query", selection);
        return 1;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub
        Log.e(TAG, "Insert called on "+myPort);
        Log.e(TAG, "isNodeDown "+isNodeDown);
        String key = values.getAsString("key");
        String value = values.getAsString("value");
        String hashKey = "";

        try {
            hashKey = genHash(key);
        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG, "NoSuchAlgorithm");
        }

        String target;
        if (nodes.lastKey().compareTo(hashKey) > 0)
            target = nodes.get(nodes.higherKey(hashKey));
        else
            target = nodes.get(nodes.firstKey());

        if(!isNodeDown) {
            new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, target, key + "\n" + value, "1");
        }
        else {
            if(!target.equals(nodeDown)) {
                new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, target, key + "\n" + value, "1");
            }
            else {
                Log.e(TAG, "Buffering");
                buffer.put(key, value);
                new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, succ1, key + "\n" + value, "2");
                new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, succ2, key + "\n" + value, "2");
            }
        }

		return null;
	}

    public void setSuccessors(String port){
        try {
            if (nodes.higherKey(genHash(ports.get(port))) != null) {
                succ1 = nodes.get(nodes.higherKey(genHash(ports.get(port))));
                if (nodes.higherKey(nodes.higherKey(genHash(ports.get(port)))) != null) {
                    succ2 = nodes.get(nodes.higherKey(nodes.higherKey(genHash(ports.get(port)))));
                } else {
                    succ2 = nodes.get(nodes.firstKey());
                }
            } else {
                succ1 = nodes.get(nodes.firstKey());
                succ2 = nodes.get(nodes.higherKey(nodes.firstKey()));
            }
        } catch (NoSuchAlgorithmException e){
            Log.e(TAG, "Nosuchalgorithm");
        }
    }

    public boolean writeFile(String key, String value){
        FileOutputStream outputStream;
        try {
            outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
            outputStream.write(value.getBytes());
            outputStream.close();
            return true;
        } catch (Exception e) {
            Log.e(TAG, "File write failed");
            return false;
        }
    }

    public MatrixCursor readFile(String selection){
        MatrixCursor matrixCursor = new MatrixCursor(columnNames);
        FileInputStream inputStream;
        String message;
        try {
            inputStream = getContext().openFileInput(selection);
            BufferedReader in = new BufferedReader(new InputStreamReader(inputStream));
            if ((message = in.readLine()) != null) {
                in.close();
                inputStream.close();
                String[] row = {selection, message};
                matrixCursor.addRow(row);
                return matrixCursor;
            }
        } catch (Exception e) {
            Log.e(TAG, "File read failed");
        }
        return null;
    }

    public MatrixCursor readAllFiles(){
        MatrixCursor matrixCursor = new MatrixCursor(columnNames);
        FileInputStream inputStream;
        String[] files = getContext().fileList();
        String message;
        for (String file :
                files) {
            try {
                inputStream = getContext().openFileInput(file);
                BufferedReader in = new BufferedReader(new InputStreamReader(inputStream));
                if ((message = in.readLine()) != null) {

                    String[] row = {file,message};
                    matrixCursor.addRow(row);
                }
                in.close();
                inputStream.close();
            } catch (Exception e) {
                Log.e(TAG,"File read failed");
            }
        }
        return matrixCursor;
    }
    
	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub

        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        ports.put("11108", "5554");
        ports.put("11112","5556");
        ports.put("11116","5558");
        ports.put("11120","5560");
        ports.put("11124","5562");

        try {
            nodes.put(genHash("5554"), "11108");
            nodes.put(genHash("5556"), "11112");
            nodes.put(genHash("5558"), "11116");
            nodes.put(genHash("5560"), "11120");
            nodes.put(genHash("5562"), "11124");
            myHash = genHash(ports.get(myPort));
        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG, "NoSuchAlgorithm");
        }

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
            return false;
        }

        new OnCreateTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR);

        return false;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub

        String message;
        MatrixCursor matrixCursor = new MatrixCursor(columnNames);

        if(selection.equals("*")){
            Log.e(TAG, "* called on "+myPort);

            matrixCursor = readAllFiles();
            for (Map.Entry<String, String> node :
                    nodes.entrySet()) {
                if (!node.getValue().equals(myPort)) {
                    try {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(node.getValue()));

                        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                        out.println("4");
                        out.flush();
                        Log.e(TAG, "Query send to " + node.getValue());
                        if ((message = in.readLine()) != null) {
                            int count = Integer.parseInt(message);
                            while (count>0){
                                if ((message = in.readLine()) != null) {
                                    String key = message;
                                    if ((message = in.readLine()) != null) {
                                        String value = message;
                                        String[] row = {key, value};
                                        matrixCursor.addRow(row);
                                    }
                                }
                                count--;
                            }
                        }
                    } catch (IOException e){
                        Log.e(TAG, "IOException at Query");
                    }
                }
            }
            return  matrixCursor;
        }
        else if(selection.equals("@")){
            return readAllFiles();
        }
        else {

            String hashKey = "";
            String port = "";
            try {
                hashKey = genHash(selection);
            } catch (NoSuchAlgorithmException e) {
                Log.e(TAG, "NoSuchAlgorithm");
            }

            if(nodes.ceilingKey(hashKey)!=null) {
                if (myHash.compareTo(nodes.ceilingKey(hashKey)) == 0) {
                    return readFile(selection);
                }
                else {
                    port = nodes.get(nodes.ceilingKey(hashKey));
                    if(isNodeDown && port.equals(nodeDown)){
                        port = succ2;
                    }
                }
            }
            else {
                if (myHash.compareTo(nodes.firstKey()) == 0) {
                    return readFile(selection);
                }
                port = nodes.get(nodes.firstKey());
                if(isNodeDown && port.equals(nodeDown)){
                    port = succ2;
                }
            }

            Log.e(TAG, "Query state " + port+" "+isNodeDown+" "+nodeDown+" "+succ2);
            try {
                Socket socket = new Socket();
                socket.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(port)));
                socket.setSoTimeout(500);

                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                out.println("3\n" + selection);
                out.flush();
                Log.e(TAG, "Query send to " + port);
                if ((message = in.readLine()) != null) {
                    String[] row = {selection, message};
                    matrixCursor.addRow(row);
                }
                else
                    throw new SocketTimeoutException();
            } catch (SocketTimeoutException e) {
                Log.e(TAG, "SocketTimeoutException has occurred - Client");
                Log.e(TAG, "Port down " + port);
                isNodeDown = true;
                nodeDown = port;
                setSuccessors(port);
                try {
                    Socket socket = new Socket();
                    socket.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(succ2)));

                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                    BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                    out.println("3\n" + selection);
                    out.flush();
                    Log.e(TAG, "Query send to " + port);
                    if ((message = in.readLine()) != null) {
                        String[] row = {selection, message};
                        matrixCursor.addRow(row);
                    }
                } catch (IOException e1){
                    Log.e(TAG, "IOException at Query");
                }

            } catch (IOException e){
                Log.e(TAG, "IOException at Query");
            }
        }
        return matrixCursor;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
        @Override
        protected Void doInBackground(ServerSocket... sockets) {

            ServerSocket serverSocket = sockets[0];

            while(true) {
                try {
                    Socket server;
                    server = serverSocket.accept();
                    String message;

                    BufferedReader in = new BufferedReader(new InputStreamReader(server.getInputStream()));
                    PrintWriter out = new PrintWriter(server.getOutputStream(), true);

                    if ((message = in.readLine()) != null) {
                        int intType = Integer.parseInt(message);
                        switch (intType){
                            case 1:
                                if ((message = in.readLine()) != null) {
                                    String key = message;
                                    if ((message = in.readLine()) != null) {
                                        String value = message;
                                        if(writeFile(key, value)) Log.e(TAG, key+" inserted on "+myPort);
                                        out.println("0");
                                        out.flush();
                                        publishProgress(key, value);
                                    }
                                }
                                break;
                            case 2:
                                if ((message = in.readLine()) != null) {
                                    String key = message;
                                    if ((message = in.readLine()) != null) {
                                        String value = message;
                                        if(writeFile(key, value)) Log.e(TAG, key+" inserted on "+myPort);
                                    }
                                    out.println("0");
                                    out.flush();
                                }
                                break;
                            case 3:
                                if ((message = in.readLine()) != null) {
                                    String key = message;
                                    Cursor cursor = readFile(key);
                                    cursor.moveToFirst();
                                    String value = cursor.getString(1);
                                    out.println(value);
                                    out.flush();
                                }
                                break;
                            case 4:
                                Cursor cursor = readAllFiles();
                                int count = cursor.getCount();
                                out.println(count);
                                out.flush();
                                cursor.moveToFirst();
                                while (!cursor.isAfterLast()){
                                    String key = cursor.getString(0);
                                    String value = cursor.getString(1);
                                    out.println(key+"\n"+value);
                                    out.flush();
                                    cursor.moveToNext();
                                }
                                break;
                            case 5:
                                if ((message = in.readLine()) != null) {

                                    Log.e(TAG,"Node down"+message);
                                    isNodeDown = true;
                                    nodeDown = message;
                                    setSuccessors(nodeDown);
                                }
                                break;
                            case 6:
                                if(isNodeDown) {
                                    if ((message = in.readLine()) != null) {
                                        if(nodeDown.equals(message)) {

                                            Log.e(TAG,"Node Up"+nodeDown);
                                            isNodeDown = false;
                                            nodeDown = "";
                                            out.println(buffer.size());
                                            out.flush();
                                            for (Map.Entry<String, String> item :
                                                    buffer.entrySet()) {
                                                out.println(item.getKey()+"\n"+item.getValue());
                                                out.flush();
                                            }
                                            buffer.clear();

                                        }
                                    }
                                }
                                else {
                                    out.println("No");
                                    out.flush();
                                }
                                break;
                        }
                    }
                } catch (IOException e) {
                    Log.e(TAG, "IOException at ServerTask");
                }
            }
        }

        protected void onProgressUpdate(String... v) {
            String key = v[0];
            String value = v[1];

            if (myHash.equals(nodes.lastKey())){

                if(isNodeDown && nodes.get(nodes.firstKey()).equals(nodeDown)) {
                    Log.e(TAG, "Check1 "+nodes.get(nodes.firstKey())+" "+nodeDown);
                    Log.e(TAG, "Buffering");
                    buffer.put(key, value);
                }
                else
                    new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, nodes.get(nodes.firstKey()), key+"\n"+value, "2");


                if(isNodeDown && nodes.get(nodes.higherKey(nodes.firstKey())).equals(nodeDown)) {
                    Log.e(TAG, "Check2 "+nodes.get(nodes.higherKey(nodes.firstKey()))+" "+nodeDown);
                    Log.e(TAG, "Buffering");
                    buffer.put(key, value);
                }
                else
                    new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, nodes.get(nodes.higherKey(nodes.firstKey())), key + "\n" + value, "2");
            }
            else if(nodes.higherKey(myHash).equals(nodes.lastKey())) {

                if(isNodeDown && nodes.get(nodes.lastKey()).equals(nodeDown)) {
                    Log.e(TAG, "Check3 "+nodes.get(nodes.lastKey())+" "+nodeDown);
                    Log.e(TAG, "Buffering");
                    buffer.put(key, value);
                }
                else
                    new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, nodes.get(nodes.lastKey()), key + "\n" + value, "2");

                if(isNodeDown && nodes.get(nodes.firstKey()).equals(nodeDown)) {
                    Log.e(TAG, "Check4 "+nodes.get(nodes.firstKey())+" "+nodeDown);
                    Log.e(TAG, "Buffering");
                    buffer.put(key, value);
                }
                else
                    new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, nodes.get(nodes.firstKey()), key + "\n" + value, "2");
            }
            else {

                if(isNodeDown && nodes.get(nodes.higherKey(myHash)).equals(nodeDown)) {
                    Log.e(TAG, "Check5 " + nodes.get(nodes.higherKey(myHash)) + " " + nodeDown);
                    Log.e(TAG, "Buffering");
                    buffer.put(key, value);
                }
                else
                    new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, nodes.get(nodes.higherKey(myHash)), key+"\n"+value, "2");


                if(isNodeDown && nodes.get(nodes.higherKey(nodes.higherKey(myHash))).equals(nodeDown)) {
                    Log.e(TAG, "Check6 "+nodes.get(nodes.higherKey(nodes.higherKey(myHash)))+" "+nodeDown);
                    Log.e(TAG, "Buffering");
                    buffer.put(key, value);
                }
                else
                    new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, nodes.get(nodes.higherKey(nodes.higherKey(myHash))), key+"\n"+value, "2");
            }
        }
    }

    private class ClientTask extends AsyncTask<String, String, Void> {

        @Override
        protected Void doInBackground(String... msgs) {

            String port = msgs[0];
            String msgToSend = msgs[1];
            String type = msgs[2];
            String message = "";
            int intType = Integer.parseInt(type);

            try {
                Socket socket = new Socket();
                socket.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(port)));
                socket.setSoTimeout(500);

                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                switch (intType) {
                    case 1:
                        out.println("1\n" + msgToSend);
                        out.flush();
                        Log.e(TAG, "Insert send to Coordinator " + port +" with msg "+msgToSend);
                        if ((message = in.readLine()) != null) {
                            Log.e(TAG,"Okay "+message);
                        }
                        else
                            throw new SocketTimeoutException();
                        break;
                    case 2:
                        out.println("2\n" + msgToSend);
                        out.flush();
                        Log.e(TAG, "Insert send to Successor " + port +" with msg "+msgToSend);
                        if ((message = in.readLine()) != null) {
                            Log.e(TAG,"Okay "+message);
                        }
                        else
                            throw new SocketTimeoutException();
                        break;
                }

                in.close();
                out.close();
                socket.close();
            } catch (SocketTimeoutException e) {
                Log.e(TAG, "SocketTimeoutException has occurred - Client");
                Log.e(TAG,"Port down "+port);
                isNodeDown = true;
                nodeDown = port;
                setSuccessors(port);
                Log.e(TAG, "Succ " + succ1 + " " + succ2);
                String[] split = msgToSend.split("[\\r\\n]+");
                Log.e(TAG, "Split " + split[0] + " " + split[1]);
                buffer.put(split[0], split[1]);
                if(intType==1) {
                    try {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(succ1));
                        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                        out.println("2\n" + msgToSend);

                        if ((message = in.readLine()) != null) {
                            Log.e(TAG, "Okay " + message);
                        }

                        in.close();
                        out.close();
                        socket.close();

                    } catch (IOException e1) {
                        Log.e(TAG, "IOException at ClientTask");
                    }
                    try {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(succ2));
                        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);

                        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                        out.println("2\n" + msgToSend);

                        if ((message = in.readLine()) != null) {
                            Log.e(TAG, "Okay " + message);
                        }

                        in.close();
                        out.close();
                        socket.close();

                    } catch (IOException e1) {
                        Log.e(TAG, "IOException at ClientTask");
                    }
                }
                for (Map.Entry<String, String> node :
                        nodes.entrySet()) {
                    if (!node.getValue().equals(myPort) && !node.getValue().equals(port)) {
                        try {
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(node.getValue()));
                            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                            Log.e(TAG, "Nodedown msg sent to "+node.getValue());
                            out.println("5\n"+ port);
                            out.close();
                            socket.close();

                        } catch (IOException e1) {
                            Log.e(TAG, "IOException at ClientTask");
                        }
                    }
                }
            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e){
                Log.e(TAG, "IOException at ClientTask");
            }

            return null;
        }

        @Override
        protected void onProgressUpdate(String... values) {
            String port = values[0];

        }
    }

    private class OnCreateTask extends AsyncTask<Void, Void, Void> {
        @Override
        protected Void doInBackground(Void... msgs) {

            String message = "";

            for (String port :
                    ports.keySet()) {
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(port));
                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);

                    BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                    out.println("6\n" + myPort);
                    out.flush();

                    if((message = in.readLine()) != null){
                        Log.e(TAG,"Revovery from "+port+" "+ message);
                        if(!message.equals("No")){
                            int count = Integer.parseInt(message);
                            while(count>0){
                                String key = "";
                                String value = "";
                                if((message = in.readLine()) != null){
                                    key = message;
                                    if((message = in.readLine()) != null){
                                        value = message;
                                        writeFile(key, value);
                                        Log.e(TAG,key+" recovered");
                                    }
                                }
                                count--;
                            }
                        }
                    }
                    out.close();
                    in.close();
                    socket.close();

                } catch (IOException e){
                    Log.e(TAG, "IOException at ClientTask");
                }
            }

            return null;
        }
    }
}
