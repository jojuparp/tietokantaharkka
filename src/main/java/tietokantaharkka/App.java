package tietokantaharkka;

import java.sql.*;
import java.util.Scanner;

public class App {

    private static String dbName = "tilausjarjestelma.db";
    private static Connection db = null;

    private static String manual =
    "\nSyötä 'quit' lopettaaksesi\n"+
    "Syötä '1' alustaaksesi tietokannan\n"+
    "Syötä '2' lisätäksesi paikan\n"+
    "Syötä '3' lisätäksesi asiakkaan\n"+
    "Syötä '4' lisätäksesi paketin\n"+
    "Syötä '5' lisätäksesi tapahtuman\n"+
    "Syötä '6' hakeaksesi tietyn paketin tapahtumat\n"+
    "Syötä '7' hakeaksesi tietyn asiakkaan paketit ja niiden tapahtumat\n"+
    "Syötä '8' hakeaksesi tietyn paikan tapahtumat tiettynä päivänä\n"+
    "Syötä '9' suorittaaksesi tietokannan tehokkuustestin\n";

    private static void connect() throws SQLException {
        db = DriverManager.getConnection("jdbc:sqlite:" + dbName);
    }

    private static void disconnect() throws SQLException {
        db.close();
    }

    private static void quit(Scanner input) throws SQLException {
        input.close();
        db.commit();
        disconnect();
        System.out.println("\nJärjestelmä suljettu.");
    }

    private static int fetchCustomerIdByName(String asiakasNimi) throws SQLException {
        int result = 0;
        try {
            PreparedStatement p = db.prepareStatement(
                "select id "+
                "from Asiakas a "+
                "where a.nimi = ?"
                );
                p.setString(1, asiakasNimi);
        
                ResultSet rows = p.executeQuery();
                result += rows.getInt("id");

        } catch (Exception e) {
            System.out.println(e);
        }
        return result;
    }

    private static int fetchParcelIdByCode(String seuKoodi) {
        int result = 0;
        try {
            PreparedStatement p = db.prepareStatement(
                "select id "+
                "from Paketti p "+
                "where p.seuKoodi = ?"
            );
            p.setString(1, seuKoodi);

            ResultSet rows = p.executeQuery();
            result += rows.getInt("id");
        } catch (Exception e) {
            System.out.println(e);
        }
        return result;
    }

    private static int fetchLocationIdByName(String nimi) {
        int result = 0;
        try {
            PreparedStatement p = db.prepareStatement(
                "select id "+
                "from Paikka p "+
                "where p.nimi = ?"
            );
            p.setString(1, nimi);

            ResultSet rows = p.executeQuery();
            result += rows.getInt("id");
        } catch (Exception e) {
            System.out.println(e);
        }
        return result;
    }

    private static void init(boolean withIndexes) throws SQLException { 
        Statement s = db.createStatement();
        db.setAutoCommit(false);

        s.execute("pragma foreign_keys = on");
        s.execute(
            "create table if not exists Asiakas ("+
            "id integer primary key, "+
            "nimi varchar(50) unique)"
        );
        s.execute(
            "create table if not exists Paketti ("+
            "id integer primary key, "+
            "asiakasId integer not null, "+
            "seuKoodi varchar(50) unique)"
        );
        s.execute(
            "create table if not exists AsiakasJaPaketti ("+
            "pakettiId integer primary key, " +
            "asiakasId integer not null references Paketti)"
        );
        s.execute(
            "create table if not exists Paikka ("+
            "id integer primary key, "+
            "nimi varchar(50) not null unique)"
        );
        s.execute(
            "create table if not exists Tapahtuma ("+
            "pakettiId integer not null references Paketti, "+
            "paikkaId integer not null references Paikka, "+
            "kuvaus varchar(50), "+
            "paivaJaAika datetime, "+
            "primary key (pakettiId, paikkaId, kuvaus))"
        );
        
        if (withIndexes) {
            s.execute("create index idx_seuKoodi on Paketti (seuKoodi)");
        } 

        System.out.println("\nTalut luotu.\n");
    }

    private static void addLocation(Scanner input) throws SQLException {
        System.out.println("\nAnna paikan nimi:");
        String nimi = input.nextLine();

        try {
            PreparedStatement p = db.prepareStatement("insert into Paikka(nimi) values(?)");
            p.setString(1, nimi);
            p.executeUpdate();
        } catch (Exception e) {
            System.out.println(e);
        }

        System.out.println("Paikka lisätty.");
    }

    private static void addCustomer(Scanner input) throws SQLException {
        System.out.println("\nAnna asiakkaan nimi:");
        String nimi = input.nextLine();

        try {
            PreparedStatement p = db.prepareStatement("insert into Asiakas(nimi) values (?)");
            p.setString(1, nimi);
            p.executeUpdate();
        } catch (Exception e) {
            System.out.println(e);
            return;
        }

        System.out.println(nimi + " lisätty\n");
    }

    private static void addParcel(Scanner input) throws SQLException {
        System.out.println("\nAnna paketin seurantakoodi:");
        String seuKoodi = input.nextLine();
        System.out.println("\nAnna asiakkaan nimi:");
        String nimi = input.nextLine();
        int asiakasId = fetchCustomerIdByName(nimi);

        if (asiakasId < 1) throw new SQLDataException("Asiakasta ei löydy tietokannasta.");

        try {
            PreparedStatement p = db.prepareStatement("insert into Paketti(seuKoodi, asiakasId) values(?, ?)");
            p.setString(1, seuKoodi);
            p.setInt(2, asiakasId);
            p.executeUpdate();

            addParcelForCustomer(asiakasId);
        } catch (Exception e) {
            System.out.println(e);
        }
        System.out.println("Paketti lisätty.");
    }

    private static void addParcelForCustomer(int asiakasId) throws SQLException {
        try {
            PreparedStatement p = db.prepareStatement(
                "insert into AsiakasJaPaketti(asiakasId, pakettiId) "+
                "values (?,last_insert_rowid())"
            );
            p.setInt(1, asiakasId);
            p.executeUpdate();
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    private static void addEvent(Scanner input) throws SQLException{
        System.out.println("\nAnna paketin seurantakoodi:");
        String seuKoodi = input.nextLine();
        System.out.println("\nAnna paikan nimi:");
        String paikanNimi = input.nextLine();
        System.out.println("\nAnna tapahtumalle kuvaus:");
        String kuvaus = input.nextLine();

        int pakettiId = fetchParcelIdByCode(seuKoodi);
        if (pakettiId < 1) throw new SQLDataException("Pakettia ei ole tietokannassa.");
        int paikkaId = fetchLocationIdByName(paikanNimi);
        if (paikkaId < 1) throw new SQLDataException("Paikkaa ei ole tietokannassa.");

        try {
            PreparedStatement p = db.prepareStatement(
                "insert into Tapahtuma(pakettiId, paikkaId, kuvaus, paivaJaAika) "+
                "values (?,?,?,datetime('now'))"
            );
            p.setInt(1, pakettiId);
            p.setInt(2, paikkaId);
            p.setString(3, kuvaus);
            p.executeUpdate();
        } catch (Exception e) {
            System.out.println(e);
        }
        System.out.println("\nTapahtuma kirjattu.");
    }

    private static void fetchParcelEvents(Scanner input) throws SQLException {
        System.out.println("\nAnna paketin seurantakoodi:");
        String seuKoodi = input.nextLine();

        int pakettiId = fetchParcelIdByCode(seuKoodi);
        if (pakettiId < 1) throw new SQLDataException("Pakettia ei ole tietokannassa. Tarkista seurantakoodi.");

        try {
            PreparedStatement p = db.prepareStatement(
                "select t.paivaJaAika, t.kuvaus "+
                "from Tapahtuma t, Paketti p "+
                "where p.id = ?"
            );
            p.setInt(1, pakettiId);
            ResultSet rows = p.executeQuery();
            System.out.println("Päivämäärä ja aika|Kuvaus");
            while (rows.next()) {
                System.out.println(rows.getString("paivaJaAika") + "|" + rows.getString("kuvaus"));
            }
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    private static void fetchCustomerParcels(Scanner input) throws SQLException {
        System.out.println("\nAnna asiakkaan nimi:");
        String nimi = input.nextLine();

        int asiakasId = fetchCustomerIdByName(nimi);
        if (asiakasId < 1) throw new SQLDataException("Asiakasta ei löydy tietokannasta.");

        try {
            PreparedStatement p = db.prepareStatement(
                "select ap.pakettiId, count(*) as lkm "+
                "from AsiakasJaPaketti ap left join Tapahtuma t on ap.pakettiId = t.pakettiId "+
                "where ap.asiakasId = ? "+
                "group by t.pakettiId"
            );
            p.setInt(1, asiakasId);
            ResultSet rows = p.executeQuery();
            System.out.println("PakettiId|Tapahtumat(kpl)");
            while(rows.next()) {
                System.out.println(rows.getInt("pakettiId") + "|" + rows.getInt("lkm"));
            }
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    private static void fetchLocationEvents(Scanner input) throws SQLException {
        System.out.println("\nAnna paikan nimi:");
        String paikka = input.nextLine();

        int paikkaId = fetchLocationIdByName(paikka);
        if (paikkaId < 1) throw new SQLDataException("Paikkaa ei löydy tietokannasta.");

        try {
            PreparedStatement p = db.prepareStatement(
                "select count(*) as lkm, date(t.paivaJaAika) as pvm "+
                "from Tapahtuma t "+
                "where t.paikkaId = ? "+
                "group by date(t.paivaJaAika)"
            );
            p.setInt(1, paikkaId);
            ResultSet rows = p.executeQuery();
            System.out.println("Tapahtumat kpl|Päivämäärä");
            while (rows.next()) {
                System.out.println(rows.getInt("lkm") + "|" + rows.getString("pvm"));
            }
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    private static void benchmark() throws SQLException {
        long start = System.currentTimeMillis();

        PreparedStatement p = db.prepareStatement("begin transaction");
        p.execute();
        p.clearParameters();
        String nimi = "peksi";


        long stop = System.currentTimeMillis();
    }

    // TODO: virheenkäsittelyt kaikille!
    // paketilta id pois ja seuKoodi yksilöiväksi. Samoin Asiakkaalle nimi. Samoin Paikalle nimi.

    public static void main(String[] args) throws SQLException {
        connect();
        
        System.out.println("\nTervetuloa tietokantajärjestelmään. \n\n Näytä käyttöohjeet: syötä 'man' \n Lopeta ohjelma: syötä 'quit'\n\n");

        Scanner input = new Scanner(System.in);
        System.out.print("Valitse toiminto(1-9): ");

        while(input.hasNextLine()) {
            String inputString = input.nextLine();

            if(inputString.equals("quit")) break;

            switch(inputString) {
                case "man": System.out.println(manual); break;

                case "1": init(false); break;

                case "2": addLocation(input); break;

                case "3": addCustomer(input); break;

                case "4": addParcel(input); break;

                case "5": addEvent(input); break;

                case "6": fetchParcelEvents(input); break;

                case "7": fetchCustomerParcels(input); break;

                case "8": fetchLocationEvents(input); break;

                case "9": benchmark(); break;

                default: System.out.println(manual); break;
            }
            System.out.print("\nValitse toiminto(1-9): ");
        }
    quit(input);
    }
}
