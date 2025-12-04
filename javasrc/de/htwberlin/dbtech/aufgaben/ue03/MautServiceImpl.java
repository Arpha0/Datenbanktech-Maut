package de.htwberlin.dbtech.aufgaben.ue03;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import de.htwberlin.dbtech.exceptions.DataException;
import de.htwberlin.dbtech.exceptions.AlreadyCruisedException;
import de.htwberlin.dbtech.exceptions.InvalidVehicleDataException;
import de.htwberlin.dbtech.exceptions.UnkownVehicleException;

/**
 * Die Klasse realisiert den AusleiheService.
 * 
 * @author Patrick Dohmeier
 */
public class MautServiceImpl implements IMautService {

	private static final Logger L = LoggerFactory.getLogger(MautServiceImpl.class);
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
	public void berechneMaut(int mautAbschnitt, int achszahl, String kennzeichen)
			throws UnkownVehicleException, InvalidVehicleDataException, AlreadyCruisedException {
		// 1. ist das Fahrzeug bekannt?
		boolean istAutoRegistriert = istAutoRegistriert(kennzeichen);
		boolean istManuellRegistriert = istManuellRegistriert(mautAbschnitt, kennzeichen);
		// TODO weitere Logik
		if(!istAutoRegistriert && !istManuellRegistriert) {
			throw new UnkownVehicleException("Fahrzeug nicht registriert");
		}

		int gespeicherteAchsen = 0;
		if (istAutoRegistriert) {
			gespeicherteAchsen = getAchsenFuerFahrzeug(kennzeichen);
		} else {
			try {
				gespeicherteAchsen = getAchsenFuerBuchung(mautAbschnitt, kennzeichen);
			} catch (DataException e) {
				if (hatAbgeschlosseneBuchung(mautAbschnitt, kennzeichen)) {
					throw new AlreadyCruisedException("Doppelbefahrung erkannt");
				} else {
					throw e;
				}
			}
		}

		if (gespeicherteAchsen != achszahl) {
			throw new InvalidVehicleDataException("Achszahl stimmt nicht überein");
		}

	}

	private boolean hatAbgeschlosseneBuchung(int mautAbschnitt, String kennzeichen) {
		String sql = "SELECT * FROM Buchung WHERE Abschnitts_id = ? AND Kennzeichen = ? AND b_id = 3";

		try (PreparedStatement s = connection.prepareStatement(sql)){
			s.setInt(1, mautAbschnitt);
			s.setString(2, kennzeichen);

			try (ResultSet rs = s.executeQuery()){
				return rs.next();
			}
		} catch (SQLException e) {
			throw new DataException(e);
		}
	}

	private int getAchsenFuerBuchung(int mautAbschnitt, String kennzeichen){
		String sql = "SELECT k.Achszahl FROM Buchung b JOIN Mautkategorie k ON b.Kategorie_id = k.kategorie_id " +
				"WHERE b.Abschnitts_id = ? AND b.Kennzeichen = ? AND b.b_id = 1";

		try (PreparedStatement s = connection.prepareStatement(sql)){
			s.setInt(1, mautAbschnitt);
			s.setString(2, kennzeichen);

			try (ResultSet rs = s.executeQuery()){
				if (rs.next()){
					// Als String lesen (z.B. "= 4")
					String achsenString = rs.getString(1);

					// Alles entfernen, was keine Zahl ist (z.B. "=", ">", " ")
					// Alles außer Ziffern 0-9 löschen.
					String nurZahlen = achsenString.replaceAll("[^0-9]", "");

					// In int umwandeln
					return Integer.parseInt(nurZahlen);
				} else {
					throw new DataException("Offene Buchung für Fahrzeug " + kennzeichen + " nicht gefunden.");
				}
			}
		} catch (SQLException e) {
			throw new DataException(e);
		}
	}

	private int getAchsenFuerFahrzeug(String kennzeichen) {
		String sql = "SELECT Achsen FROM Fahrzeug WHERE Kennzeichen = ? AND Abmeldedatum IS NULL";

		try (PreparedStatement s = connection.prepareStatement(sql)) {
			s.setString(1, kennzeichen);

			try (ResultSet rs = s.executeQuery()) {
				if (rs.next()) {
					return rs.getInt("Achsen");
				} else {
					throw new DataException("Fahrzeug mit Kennzeichen " + kennzeichen + " nicht gefunden.");
				}
			}
		} catch (SQLException e) {
			throw new DataException(e);
		}
	}


	private boolean istManuellRegistriert(int mautAbschnitt, String kennzeichen) {
		try (PreparedStatement s = connection.prepareStatement("SELECT * FROM Buchung b" +
				" WHERE Kennzeichen = ? AND Abschnitts_Id = ?")) {
			s.setString(1, kennzeichen);
			s.setInt(2, mautAbschnitt);
			ResultSet rs = s.executeQuery();
			return rs.next();
		} catch (SQLException e){
			throw new DataException(e);
		}
	}

	// Methode prüft, ob Fahrzeug im automatischen Verfahren bekannt ist
	private boolean istAutoRegistriert(String kennzeichen) {
		boolean istAutoRegistriert = false;
		String sql = "SELECT f.FZ_ID FROM Fahrzeug f " +
				"JOIN Fahrzeuggerat fg ON f.fz_id = fg.fz_id " +
				"WHERE f.Kennzeichen = ? " +
				"AND f.Abmeldedatum IS NULL";
		try (PreparedStatement s = connection.prepareStatement(sql)) {
			s.setString(1, kennzeichen);
			try(ResultSet rs = s.executeQuery()) {
				istAutoRegistriert = rs.next();
			}
		} catch (Exception e) {
			throw new DataException(e);
		}
		return istAutoRegistriert;
	}



}
