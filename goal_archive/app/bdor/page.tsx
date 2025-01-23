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
  const [year, setYear] = useState<number>(2024); // State to store the current year
  const [data, setData] = useState<Player[] | null>(null); // State to store API data
  const [loading, setLoading] = useState<boolean>(true); // State for loading
  const [error, setError] = useState<string | null>(null); // State for errors

  useEffect(() => {
    const fetchData = async () => {
      try {
        setLoading(true);
        setError(null);
        setData(null); // Reset data while fetching
        const response = await fetch(`http://0.0.0.0:90/bdor/${year}`);
        if (!response.ok) {
          throw new Error(`Failed to fetch data for year ${year}`);
        }
        const result = await response.json();
        if (result?.data) {
          setData(result.data); // Assuming the API returns a `data` array
        } else {
          setData([]);
        }
      } catch (err) {
        setError((err as Error).message);
      } finally {
        setLoading(false);
      }
    };

    fetchData();
  }, [year]); // Trigger fetch when the year changes

  const handlePreviousYear = () => setYear((prevYear) => prevYear - 1);
  const handleNextYear = () => setYear((prevYear) => prevYear + 1);

  return (
    <div className="min-h-screen bg-black text-white pt-20 sm:pt-20 px-8 pb-14">
      {/* Header */}
      <h1 className="text-4xl sm:text-3xl font-bold text-center mb-8">
        FIFA Ballon Dor Rankings
      </h1>

      {/* Year Selector */}
      <div className="flex items-center justify-center mb-8 font-[family-name:var(--font-geist-sans)]">
        <button
          onClick={handlePreviousYear}
          className="px-4 py-2 bg-gray-800 text-white rounded-lg hover:bg-gray-700 mr-4"
        >
          Previous
        </button>
        <span className="text-2xl sm:text-3xl font-semibold">{year}</span>
        <button
          onClick={handleNextYear}
          className="px-4 py-2 bg-gray-800 text-white rounded-lg hover:bg-gray-700 ml-4"
        >
          Next
        </button>
      </div>

      {/* Loading or Error Messages */}
      {loading && <p className="text-center text-gray-400">Loading...</p>}
      {error && <p className="text-center text-red-500">{error}</p>}

      {/* Data Display */}
      {!loading && !error && data && data.length > 0 && (
        <div className="overflow-x-auto font-[family-name:var(--font-geist-sans)]">
          <table className="table-auto w-full sm:max-w-lg lg:max-w-3xl mx-auto border-collapse border border-gray-700">
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
      {!loading && !error && data && data.length === 0 && (
        <p className="text-center text-gray-400">
          No rankings available for {year}.
        </p>
      )}
    </div>
  );
}
