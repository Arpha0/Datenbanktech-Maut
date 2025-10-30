package de.htwberlin.dbtech.aufgaben.ue02;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import de.htwberlin.dbtech.exceptions.DataException;

/**
 * Die Klasse realisiert die Mautverwaltung.
 *
 * @author Patrick Dohmeier
 */
public class MautVerwaltungImpl implements IMautVerwaltung {

	private static final Logger L = LoggerFactory.getLogger(MautVerwaltungImpl.class);
	private Connection connection;

	@Override
	public void setConnection(Connection connection) {
		this.connection = connection;
	}

	private Connection getConnection() {
		if (connection == null) {
			throw new DataException("Connection not set");
		}
		return connection;
	}

    @Override
    public String getStatusForOnBoardUnit(long fzg_id) {
        String status = null;
        String sql = "SELECT STATUS FROM FAHRZEUGGERAT WHERE FZG_ID = ?";

        try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
            stmt.setLong(1, fzg_id);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    status = rs.getString("STATUS");
                }
            }
        } catch (SQLException e) {
            throw new DataException(e);
        }
        return status;
    }

    @Override
    public int getUsernumber(int maut_id) {
        int nutzer_id = 0;
        String sql = "SELECT T3.NUTZER_ID " +
                "FROM MAUTERHEBUNG T1 " +
                "JOIN FAHRZEUGGERAT T2 ON T1.FZG_ID = T2.FZG_ID " +
                "JOIN FAHRZEUG T3 ON T2.FZ_ID = T3.FZ_ID " +
                "WHERE T1.MAUT_ID = ?";

        try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
            stmt.setInt(1, maut_id);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    nutzer_id = rs.getInt("NUTZER_ID");
                }
            }
        } catch (SQLException e) {
            throw new DataException(e);
        }
        return nutzer_id;
    }

    @Override
    public void registerVehicle(long fz_id, int sskl_id, int nutzer_id, String kennzeichen, String fin, int achsen,
                                int gewicht, String zulassungsland) {

        String sql = "INSERT INTO FAHRZEUG " +
                "(FZ_ID, SSKL_ID, NUTZER_ID, KENNZEICHEN, FIN, ACHSEN, GEWICHT, ZULASSUNGSLAND, ANMELDEDATUM) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)";

        try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
            stmt.setLong(1, fz_id);
            stmt.setInt(2, sskl_id);
            stmt.setInt(3, nutzer_id);
            stmt.setString(4, kennzeichen);
            stmt.setString(5, fin);
            stmt.setInt(6, achsen);
            stmt.setInt(7, gewicht);
            stmt.setString(8, zulassungsland);
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new DataException(e);
        }
    }

    @Override
    public void updateStatusForOnBoardUnit(long fzg_id, String status) {
        String sql = "UPDATE FAHRZEUGGERAT SET STATUS = ? WHERE FZG_ID = ?";
        try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
            stmt.setString(1, status);
            stmt.setLong(2, fzg_id);
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new DataException(e);
        }
    }

    @Override
    public void deleteVehicle(long fz_id) {
        String sql = "DELETE FROM FAHRZEUG WHERE FZ_ID = ?";
        try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
            stmt.setLong(1, fz_id);
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new DataException(e);
        }
    }

    @Override
    public List<Mautabschnitt> getTrackInformations(String abschnittstyp) {
        List<Mautabschnitt> result = new ArrayList<>();
        String sql = "SELECT ABSCHNITTS_ID, LAENGE, START_KOORDINATE, ZIEL_KOORDINATE, NAME, ABSCHNITTSTYP "
                + "FROM MAUTABSCHNITT WHERE ABSCHNITTSTYP = ?";
        try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
            stmt.setString(1, abschnittstyp);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    Mautabschnitt abschnitt = new Mautabschnitt(
                            rs.getInt("ABSCHNITTS_ID"),
                            rs.getInt("LAENGE"),
                            rs.getString("START_KOORDINATE"),
                            rs.getString("ZIEL_KOORDINATE"),
                            rs.getString("NAME"),
                            rs.getString("ABSCHNITTSTYP"));
                    result.add(abschnitt);
                }
            }
        } catch (SQLException e) {
            throw new DataException(e);
        }
        return result;
    }
}
