/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gestion;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;
import org.jdom2.output.Format;
import org.jdom2.output.XMLOutputter;

/**
 *
 * @author Isaias
 */
public class LogginDAO {
    
    private final String dbURL; // =  "jdbc:mysql://localhost/NOMBREBBDD";
    private final String dbName; //= "root";
    private final String dbPass; //= "";
    private Connection connection = null;
    private Statement statement = null;
    private ResultSet resultSet = null;
    
    public LogginDAO (String dbURL, String dbName, String dbPass){
        this.dbURL = dbURL;
        this.dbName = dbName;
        this.dbPass = dbPass;
    }
    
    /**
    * Este metodo se usa para leer un archivo XML y recoger todas las etiquetas para
    * luego volcar esos datos en la tabla indicada
    * 
    * @param ficheroXML Fichero XML del que lee los datos
    * @param tabla Tabla de la BBDD donde vuelca los datos del fichero
    */
    public void transformarXmlToDB(String ficheroXML,String tabla){
        //Primero extraemos los datos del XML y lo guardamos en un ArrayList
        ArrayList <Map> consulta = leerXML(ficheroXML,tabla);
        //Luego insertamos los datos guardados del ArrayList anterior
        insertarDatos(tabla,consulta);
    }
    
    /**
    * Este metodo se usa para leer un fichero XML y volcar sus datos en una tabla especifica.
    * @param ficheroXML Fichero XML donde se van a escribir los datos.
    * @param tabla tabla de la BBDD.
    */
    public void transformarBDToXml(String ficheroXML,String tabla, String[] campos){
        //Primero extraemos los datos de la Base de Datos y lo guardamos en un ArrayList
        ArrayList <Map> consulta = extraerDatos(tabla,campos);
        //Luego pintamos los datos dentro del XML
        escribirXML(ficheroXML,tabla,consulta);
    }
    
    private ArrayList<Map> leerXML(String ficheroXML, String tabla){
        ArrayList <Map> consulta = new ArrayList<>();
        SAXBuilder builder = new SAXBuilder();
        File xml = new File(ficheroXML);
        try {
            Document doc = builder.build(xml);
            Element raiz = doc.getRootElement();
            List<Element> l_Registros = raiz.getChildren("l_"+tabla);
            for (Element registros : l_Registros) {
                for (Element elementos : registros.getChildren()) {
                    Map<String,String> columna = new TreeMap<>();
                    for (Element element : elementos.getChildren()) {
                        columna.put(element.getName(), element.getTextTrim());
                    }
                    consulta.add(columna);
                }
            }
        } catch (JDOMException | IOException ex) {
            System.out.println(ex.getMessage());
        }
        return consulta;
    }
    
    private void escribirXML(String ficheroXML, String tabla, ArrayList<Map> consulta){
        try {
            //Creamos la etiqueta raiz
            Element loggin = new Element("root");
            Document doc = new Document(loggin);
            
            Element l_Registros = new Element("l_"+tabla);
            loggin.addContent(l_Registros);
            // recorremos el array de la consulta de la tabla creamos la etiquetas y añadimos los valores
            for (Map <String,String> registro : consulta) {
                Element tablaElement = new Element(tabla);
                l_Registros.addContent(tablaElement);
                //Recorremos el mapa con el Key -> Nombre Columna || Value -> Contenido
                for (Map.Entry<String, String> e : registro.entrySet()) {
                    Element columna = new Element(e.getKey());
                    columna.setText(e.getValue());
                    tablaElement.addContent(columna);
                }
            }
            //Creamos el archivo XML
            XMLOutputter xml = new XMLOutputter();
            xml.setFormat(Format.getPrettyFormat());
            xml.output(doc, new FileWriter(ficheroXML));
        } catch (IOException ex) {
            System.out.println(ex.getMessage());
        }
    }
    
    private ArrayList<Map> extraerDatos(String tabla, String[] campos) {
        
        //Construimos la sentecia de consulta
        String query = "SELECT * FROM "+tabla.toLowerCase();
        ArrayList <Map> consulta = new ArrayList<>();
        try {
            openConnection();
            resultSet = statement.executeQuery(query); //Lanzamos la sentecia contra la BBDD
            while (resultSet.next()) {
                //Guardamos cada fila en el ArrayList
                Map<String,String> columna = new TreeMap<>();
                for (String campo: campos) {
                   columna.put(campo, resultSet.getString(campo));
                }
                consulta.add(columna);
            }
            resultSet.close();
            closeConnection();
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
        return consulta;
    }
    private void insertarDatos(String tabla, ArrayList<Map> consulta ){
        String columnas;
        String valores;
        //Montamos el insert con las columnas y los valores
        for (Map <String,String> registro : consulta) {
            columnas = "";
            valores = "";
            for (Map.Entry<String, String> e : registro.entrySet()) {
                columnas  += ("".equals(columnas) ? "(" : ",") + e.getKey();
                valores  += ("".equals(valores) ? "('" : ",'") + e.getValue()+"'";
            }
            String query = "INSERT INTO "+tabla.toLowerCase()+" "+ columnas + ") VALUES "+valores+" );";
            try {
                openConnection();
                statement.executeUpdate(query);
                closeConnection();
            } catch (SQLException ex) {
                System.out.println(ex.getMessage());
            }
        }
    }
    
    private void openConnection() {
        try {
            Class.forName("com.mysql.jdbc.Driver");
            connection = DriverManager.getConnection(dbURL, dbName, dbPass);
            statement = connection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        } catch (ClassNotFoundException | SQLException ex) {
            System.out.println(ex.getMessage());
        }
         
    }

    private void closeConnection() {
        try {
            statement.close();
            connection.close();
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
    }
}
