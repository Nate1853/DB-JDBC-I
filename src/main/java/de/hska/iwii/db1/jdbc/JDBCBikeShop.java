package de.hska.iwii.db1.jdbc;

// import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.Properties;

/**
 * Diese Klasse ist die Basis für Ihre Lösung. Mithilfe der
 * Methode reInitializeDB können Sie die beim Testen veränderte
 * Datenbank wiederherstellen.
 */
public class JDBCBikeShop {

    /**
     * Stellt die Datenbank aus der SQL-Datei wieder her.
     * - Alle Tabellen mit Inhalt ohne Nachfrage löschen.
     * - Alle Tabellen wiederherstellen.
     * - Tabellen mit Daten füllen.
     * <p>
     * Getestet mit MsSQL 12, MySql 8.0.8, Oracle 11g, Oracle 18 XE, PostgreSQL 11.
     * <p>
     * Das entsprechende Sql-Skript befindet sich im Ordner ./sql im Projekt.
     *
     * @param connection Geöffnete Verbindung zu dem DBMS, auf dem die
     *                   Bike-Datenbank wiederhergestellt werden soll.
     */
    public void reInitializeDB(Connection connection) {
        try {
            System.out.println("\nInitializing DB.");
            connection.setAutoCommit(true);
            String productName = connection.getMetaData().getDatabaseProductName();
            boolean isMsSql = productName.equals("Microsoft SQL Server");
            Statement statement = connection.createStatement();
            int numStmts = 0;

            // Liest den Inhalt der Datei ein.
            // String[] fileContents = new String(Files.readAllBytes(Paths.get("sql/hska_bike.sql")), StandardCharsets.UTF_8).split(";");
            String[] fileContents = Files.readString(Paths.get("sql/hska_bike.sql")).split(";");

            for (String sqlString : fileContents) {
                try {
                    // Microsoft kenn den DATE-Operator nicht.
                    if (isMsSql) {
                        sqlString = sqlString.replace(", DATE '", ", '");
                    }
                    statement.execute(sqlString);
                    System.out.print((++numStmts % 80 == 0 ? "/\n" : "."));
                } catch (SQLException e) {
                    System.out.print("\n" + sqlString.replace('\n', ' ').trim() + ": ");
                    System.out.println(e.getMessage());
                }
            }
            statement.close();
            System.out.println("\nBike database is reinitialized on " + productName +
                    "\nat URL " + connection.getMetaData().getURL()
            );
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Erstellt eine Verbindung zur lokalen PostgreSQL-Datenbank.
     *
     * @param databaseUser     Datenbank-User.
     * @param databasePassword Datenbank-Passwort.
     * @return Datenbankverbindung.
     */
    public static Connection getPostgreSQLConnection(String databaseUser, String databasePassword, String databaseName) throws ClassNotFoundException, SQLException {
        // PostgreSQL
        Class.forName("org.postgresql.Driver");

        // 2. Verbinden mit Anmelde-Daten
        Properties props = new Properties();
        props.put("user", databaseUser);
        props.put("password", databasePassword);
        return DriverManager.getConnection("jdbc:postgresql://193.196.36.21:3690/" + databaseName, props);
    }

    public Connection startG23(Connection connection) {
        try {
            connection = getPostgreSQLConnection("g10", "vmrMDpeVEU", "g10");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return connection;
    }

    public void close(Connection connection) {
        try {
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Hauptmethoden
     */

    public void printResultSet(ResultSet resultSet) {
        try {
            ResultSetMetaData rsmd = resultSet.getMetaData();
            int columnsNumber = rsmd.getColumnCount();

            // Spaltenbreite ermitteln
            int[] columnWidth = new int[rsmd.getColumnCount()];
            for (int i = 0; i < columnWidth.length; i++) {
                columnWidth[i] = Math.max(rsmd.getColumnDisplaySize(i + 1), rsmd.getColumnLabel(i + 1).length());
            }

            //  Tabellenkopf 1 (Attribute)
            for (int j = 1; j <= columnsNumber; j++) {
                if (j > 1) System.out.print("| ");
                String columnName = rsmd.getColumnName(j);
                columnName = concat(columnName, columnWidth[j - 1]);
                System.out.print(columnName + " ");
            }
            System.out.println();

            //  Tabellenkopf 2 (Typ)
            resultSet.next();
            for (int j = 1; j <= columnsNumber; j++) {
                if (j > 1) System.out.print("| ");
                String columnName = resultSet.getString(j);
                String newColumnName = "char";
                if (isInt(columnName)) {
                    newColumnName = "number";
                }
                newColumnName = concat(newColumnName, columnWidth[j - 1]);
                System.out.print(newColumnName + " ");
            }
            System.out.println();


            // Strich
            for (int j = 1; j <= columnsNumber; j++) {
                if (j > 1) System.out.print("-+-");
                String result = String.format("%-" + columnWidth[j - 1] + "s", "-").replace(' ', '-');
                result = concat(result, columnWidth[j - 1]);
                System.out.print(result);
            }
            System.out.println();

            // Tabelle
            do {
                for (int i = 1; i <= columnsNumber; i++) {
                    if (i > 1) System.out.print("| ");
                    String columnValue = resultSet.getString(i);
                    columnValue = concat(columnValue, columnWidth[i - 1]);
                    System.out.print(columnValue + " ");
                }
                System.out.println();
            } while (resultSet.next());

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void printSelectStern(String table, Connection connection) {
        printSelectStern(new String[]{table}, connection);
    }

    public void printSelectStern(String[] tables, Connection connection) {
        try {
            Statement stmt = connection.createStatement();
            for (String temp : tables) {
                String select = "Select * from " + temp + ";";
                ResultSet resultSet = stmt.executeQuery(select);
                System.out.println("/*" + temp + "/*");
                printResultSet(resultSet);
            }

            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void printSelectCustom(String select, Connection connection) {
        try {
            Statement stmt = connection.createStatement();
            ResultSet resultSet = stmt.executeQuery(select);
            printResultSet(resultSet);
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    /**
     * Hilfsmethoden
     */

    public boolean isInt(String string) {
        try {
            Integer.parseInt(string);
        } catch (NumberFormatException e) {
            return false;
        }
        return true;
    }

    public String concat(String string, int length) {
        int suffix;
        if (string == null) {
            string = "";
        }
        if (length - string.length() > 0) {
            suffix = length - string.length();
        } else {
            suffix = string.length() - length;
        }
        if (isInt(string)) {
            string = " ".repeat(Math.max(0, suffix)) + string;
        } else {
            string = string + " ".repeat(Math.max(0, suffix));
        }
        return string;
    }

    /**
     * Aufgabenmethoden
     */

    public void exercise41(Connection connection) {
        DatabaseMetaData data;
        try {
            data = connection.getMetaData();
            System.out.println("***********");
            System.out.println("Aufgabe 4.1");
            System.out.println("***********\n");
            System.out.println("Name of Database: " + connection.getCatalog());
            System.out.println("JDBC-Driver: " + data.getDriverName());
            System.out.println("DatabaseProductVersion: " + data.getDatabaseProductVersion() + "\n");
            System.out.println("***********");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void exercise42(Connection connection) {
        System.out.println("Aufgabe 4.2");
        System.out.println("***********\n");

        printSelectStern("Kunde", connection);
        System.out.println("/*Kunde_custom*/");
        printSelectCustom("SELECT Persnr, Name, Ort, Aufgabe FROM personal", connection);
        System.out.println("\n***********");
    }

    public void exercise43(Connection connection) {
        try {
            System.out.println("Aufgabe 4.3");
            System.out.println("***********\n");
            String select = "SELECT distinct Kunde.name, Kunde.nr, Lieferant.name, Lieferant.nr" +
                    " FROM Lieferant" +
                    " FULL OUTER JOIN Lieferung on Lieferant.nr = Lieferung.Liefnr" +
                    " FULL OUTER JOIN Teilestamm on Lieferung.Teilnr = Teilestamm.Teilnr" +
                    " FULL OUTER JOIN Auftragsposten on Teilestamm.teilnr = Auftragsposten.teilnr" +
                    " FULL OUTER JOIN Auftrag on Auftragsposten.Auftrnr = Auftrag.Auftrnr" +
                    " JOIN Kunde on Auftrag.Kundnr = Kunde.nr" +
                    " order by Kunde.name asc";
            System.out.println("5-JOIN-Tabelle-stmt");
            printSelectCustom(select, connection);

            String select2 = "SELECT distinct kunde.name, kunde.nr, lieferant.name, lieferant.nr" +
                    " FROM lieferant" +
                    " FULL OUTER JOIN lieferung on lieferant.nr = lieferung.liefnr" +
                    " FULL OUTER JOIN teilestamm on lieferung.teilnr = teilestamm.teilnr" +
                    " FULL OUTER JOIN auftragsposten on teilestamm.teilnr = auftragsposten.teilnr" +
                    " FULL OUTER JOIN auftrag on auftragsposten.auftrnr = auftrag.auftrnr" +
                    " JOIN kunde on auftrag.kundnr = kunde.nr" +
                    " WHERE kunde.name like ?" +
                    " order by kunde.name asc";

            System.out.println("5-JOIN-Tabelle-PreparedStmt");

            PreparedStatement stmt2 = connection.prepareStatement(select2);
            stmt2.setString(1, "Rafa%");
            stmt2.addBatch();
            ResultSet resultSet2 = stmt2.executeQuery();
            printResultSet(resultSet2);
            stmt2.close();

            System.out.println("\n***********");

        } catch (SQLException e) {
            e.printStackTrace();
        }
        /*
         * Antwort Teil 2
         * Es muss mindestens ein PreparedStatement verwendet werden, um vor
         * SQL-Injections geschützt zu sein
         */

    }

    public void exercise44(Connection connection) {
        try {
            System.out.println("Aufgabe 4.4");
            System.out.println("***********\n");
            // Kunde
            String neuerKunde = "HAZARD-GMBH";
            String neueStrasse = "Willy-Andreas-Alle 3";
            int neuePLZ = 76131;
            String neuerOrt = "Karlsruhe";
            int neueSperre = 0;
            // Auftrag
            Date neuesAuftragsDatum = new java.sql.Date(2022-01-13);
            int neueAuftragsNummer = 6;
            int neueKundenNummer = 7;
            int neuePersonenNummer = 1;
            // Auftragsposten
            int neuerAPposnr = 53;
            int neuerAPauftrnr = 6;
            int neuerAPteilnr = 100001;
            int neuerAPanzahl = 5;
            int neuerAPgesamtpreis = 3500;


            // INSERT
            System.out.print("INSERT:");
            String insert = "Begin;" +
                    "INSERT INTO Kunde VALUES (?, ?, ?, ?, ?, ?);" +
                    "INSERT INTO Auftrag VALUES (?, ?, ?, ?);" +
                    "INSERT INTO Auftragsposten VALUES (?,?,?,?,?);" +
                    "commit;";
            PreparedStatement stmt = connection.prepareStatement(insert);
            printSelectStern(new String[]{"Kunde", "Auftrag", "Auftragsposten"}, connection);
            //Kunde
            stmt.setInt(1, neueKundenNummer);
            stmt.setString(2, neuerKunde);
            stmt.setString(3, neueStrasse);
            stmt.setInt(4, neuePLZ);
            stmt.setString(5, neuerOrt);
            stmt.setInt(6, neueSperre);
            //Auftrag
            stmt.setInt(7, neueAuftragsNummer);
            stmt.setDate(8, neuesAuftragsDatum);
            stmt.setInt(9, neueKundenNummer);
            stmt.setInt(10, neuePersonenNummer);
            //Auftragsposten
            stmt.setInt(11, neuerAPposnr);
            stmt.setInt(12, neuerAPauftrnr);
            stmt.setInt(13, neuerAPteilnr);
            stmt.setInt(14, neuerAPanzahl);
            stmt.setInt(15, neuerAPgesamtpreis);
            stmt.execute();
            printSelectStern(new String[]{"Kunde", "Auftrag", "Auftragsposten"}, connection);


            // UPDATE
            System.out.print("UPDATE:");
            String update = "Begin;" +
                    "UPDATE Kunde SET Sperre = 1 WHERE name = ?;" +
                    "commit;";
            stmt = connection.prepareStatement(update);
            printSelectStern("Kunde", connection);
            stmt.setString(1, neuerKunde);
            stmt.execute();
            printSelectStern("Kunde", connection);

            // DELETE
            System.out.print("DELETE: ");
            String delete = "BEGIN;" +
                    "DELETE FROM Auftragsposten WHERE auftrnr = ?;" +
                    "DELETE FROM Auftrag WHERE kundnr = ?;" +
                    "DELETE FROM Kunde WHERE name = ?;" +
                    "commit;";
            stmt = connection.prepareStatement(delete);
            printSelectStern(new String[]{"Kunde", "Auftrag", "Auftragsposten"}, connection);
            stmt.setInt(1, neueAuftragsNummer);
            stmt.setInt(2, neueKundenNummer);
            stmt.setString(3, neuerKunde);
            stmt.execute();
            printSelectStern(new String[]{"Kunde", "Auftrag", "Auftragsposten"}, connection);

            System.out.println("\n****");
            System.out.println("ENDE");
            System.out.println("****\n");
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    /**
     * Main
     */

    public static void main(String[] args) {
        JDBCBikeShop jdbcBikeShop = new JDBCBikeShop();
        Connection connectG23 = jdbcBikeShop.startG23(null);


        jdbcBikeShop.reInitializeDB(connectG23);

        jdbcBikeShop.close(connectG23);
    }
}
