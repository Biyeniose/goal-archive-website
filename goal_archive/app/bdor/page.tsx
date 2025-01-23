"use client";
import React, { useEffect, useState } from "react";

interface Player {
  rank: number;
  player_name: string;
  nationality: string;
  clubs: string;
  year: number;
}

export default function Page() {
  const [data, setData] = useState<Player[]>([]); // State to store API data
  const [loading, setLoading] = useState<boolean>(true); // State for loading
  const [error, setError] = useState<string | null>(null); // State for errors

  useEffect(() => {
    const fetchData = async () => {
      try {
        const response = await fetch("http://localhost:90/bdor/2024");
        if (!response.ok) {
          console.log(response);
          throw new Error("Error when fetching");
        }
        const result = await response.json();
        setData(result.data); // Assuming the API returns a `data` array
      } catch (err) {
        setError((err as Error).message);
      } finally {
        setLoading(false);
      }
    };

    fetchData();
  }, []);

  return (
    <div className="min-h-screen bg-black text-white p-8 pb-20 sm:p-20">
      {/* Header */}
      <h1 className="text-4xl sm:text-5xl font-bold text-center mb-8">
        FIFA Ballon Dor Rankings
      </h1>

      {/* Loading or Error Messages */}
      {loading && <p className="text-center text-gray-400">Loading...</p>}
      {error && <p className="text-center text-red-500">{error}</p>}

      {/* Data Display */}
      {!loading && !error && data.length > 0 && (
        <div className="overflow-x-auto">
          <table className="table-auto w-full border-collapse border border-gray-700">
            <thead className="bg-gray-800">
              <tr>
                <th className="px-4 py-2 border border-gray-700 text-left">
                  Rank
                </th>
                <th className="px-4 py-2 border border-gray-700 text-left">
                  Player Name
                </th>
                <th className="px-4 py-2 border border-gray-700 text-left">
                  Nationality
                </th>
                <th className="px-4 py-2 border border-gray-700 text-left">
                  Clubs
                </th>
                <th className="px-4 py-2 border border-gray-700 text-left">
                  Year
                </th>
              </tr>
            </thead>
            <tbody>
              {data.map((player) => (
                <tr key={`${player.rank}-${player.player_name}`}>
                  <td className="px-4 py-2 border border-gray-700">
                    {player.rank}
                  </td>
                  <td className="px-4 py-2 border border-gray-700">
                    {player.player_name}
                  </td>
                  <td className="px-4 py-2 border border-gray-700">
                    {player.nationality}
                  </td>
                  <td className="px-4 py-2 border border-gray-700">
                    {player.clubs}
                  </td>
                  <td className="px-4 py-2 border border-gray-700">
                    {player.year}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {/* No Data State */}
      {!loading && !error && data.length === 0 && (
        <p className="text-center text-gray-400">No rankings available.</p>
      )}
    </div>
  );
}
