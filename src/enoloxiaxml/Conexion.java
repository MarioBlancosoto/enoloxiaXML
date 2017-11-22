package enoloxiaxml;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamWriter;

public class Conexion {

    XMLOutputFactory factory = XMLOutputFactory.newInstance();
    XMLInputFactory fic = XMLInputFactory.newInstance();
    XMLStreamReader sr;
    String ruta = "/home/oracle/NetBeansProjects/EnoloxiaXml/analise.xml";
    XMLStreamWriter xw;
    Connection conn;
    PreparedStatement ps;

    PreparedStatement ps1;
    PreparedStatement ps2;
    ResultSet rs;
    String[] contenido = new String[4];

    public void conexion() {

        String driver = "jdbc:oracle:thin:";
        String host = "localhost.localdomain"; // tambien puede ser una ip como "192.168.1.14"
        String porto = "1521";
        String sid = "orcl";
        String usuario = "hr";
        String password = "hr";
        String url = driver + usuario + "/" + password + "@" + host + ":" + porto + ":" + sid;
        try {
            //para conectar co native protocal all java driver: creamos un obxecto Connection usando o metodo getConnection da clase  DriverManager
            conn = DriverManager.getConnection(url);
            System.out.println("Conexión realizada correctamente ");
        } catch (SQLException ex) {
            Logger.getLogger(Conexion.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    public void recogerDatos() {

        try {
            ps = conn.prepareStatement("Select * from xerado");
            rs = ps.executeQuery();
            xw = factory.createXMLStreamWriter(new FileWriter(ruta));
            xw.writeStartDocument("1.0");
            xw.writeStartElement("Analisis");
            while (rs.next()) {

                xw.writeStartElement("Analise");
                xw.writeAttribute("CodigoA", rs.getString(1));
                xw.writeStartElement("Nome_Uva");
                xw.writeCharacters(rs.getString(2));
                xw.writeEndElement();
                xw.writeStartElement("Acidez");
                xw.writeCharacters(rs.getString(3));
                xw.writeEndElement();
                xw.writeStartElement("Total");
                xw.writeCharacters(String.valueOf(rs.getInt(4)));
                xw.writeEndElement();
                xw.writeEndElement();

            }
            xw.writeEndElement();
            xw.close();
        } catch (SQLException ex) {
            Logger.getLogger(Conexion.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(Conexion.class.getName()).log(Level.SEVERE, null, ex);
        } catch (XMLStreamException ex) {
            Logger.getLogger(Conexion.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    public void ler() {
   int contador =0;
        String[] datos=new String[2];
        try {

            sr = fic.createXMLStreamReader(new FileReader(ruta));
            while (sr.hasNext()) {
                int eventType = sr.next();
                switch (eventType) {

                    case XMLStreamReader.START_DOCUMENT:
                        break;

                    case XMLStreamReader.START_ELEMENT:
                        System.out.println(sr.getLocalName());

                        if (sr.getAttributeCount() > 0) {
                            System.out.println(sr.getAttributeName(0) + " = " + sr.getAttributeValue(0));
                            contenido[3]=sr.getAttributeValue(0);
                        }
                        if(sr.getLocalName()=="tipo"){
                            contenido[0]=sr.getElementText(); 
                            datos=recogerUva(contenido[0],contenido[1]);
                        }
                        if(sr.getLocalName()=="acidez"){
                            contenido[1]=sr.getElementText();                           
                        }                        
                        if(sr.getLocalName()=="cantidade"){
                           int cuenta = Integer.parseInt(sr.getElementText())*15;
                            contenido[2]=String.valueOf(cuenta);                           
                        }

                        break;
                    case XMLStreamReader.CHARACTERS:
                        System.out.print(sr.getText());
                        
                        break;
                    case XMLStreamReader.END_ELEMENT:
                        if(sr.getLocalName()=="analise"){
                    try { 
                        ps = conn.prepareStatement("insert into xerado(num,nomeuva,tratacidez,total)values(?,?,?,?)");
                        ps.setString(1,contenido[3]);
                        ps.setString(2, datos[0]);
                        ps.setString(3, datos[1]);
                        ps.setInt(4, Integer.parseInt(contenido[2]));
                        ps.execute();
                    
                    } catch (SQLException ex) {
                        Logger.getLogger(Conexion.class.getName()).log(Level.SEVERE, null, ex);
                    }
                        }
                        break;

                }

            }

        } catch (XMLStreamException ex) {
            Logger.getLogger(Conexion.class.getName()).log(Level.SEVERE, null, ex);
        } catch (FileNotFoundException ex) {
            Logger.getLogger(Conexion.class.getName()).log(Level.SEVERE, null, ex);
        }

       
        
        
    }

    public String[] recogerUva(String tipo,String acidez) {
      String[] total = new String[2];
      String select1 = "select nomeu,acidezmin,acidezmax from uvas where tipo ='"+tipo+"'";
        try {
            ps1 = conn.prepareStatement(select1);
            rs = ps1.executeQuery();
            rs.next();
    
        
            total[0]=rs.getString("nomeu");
             if(Integer.parseInt(acidez)<Integer.parseInt(rs.getString("acidezmin"))){
                total[1]="Subir acidez";
            } else if(Integer.parseInt(acidez)>Integer.parseInt(rs.getString("acidezmax"))){
                total[1]="Bajar acidez";
        }else{
                total[1]="Equilibrada";
            }
            
            
        } catch (SQLException ex) {
            Logger.getLogger(Conexion.class.getName()).log(Level.SEVERE, null, ex);
        }
        return total;
    }

}
