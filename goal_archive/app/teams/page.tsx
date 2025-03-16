"use client";

import * as React from "react";
import AccordionGroup from "@mui/joy/AccordionGroup";
import Accordion from "@mui/joy/Accordion";
import AccordionDetails from "@mui/joy/AccordionDetails";
import AccordionSummary from "@mui/joy/AccordionSummary";

export default function Page() {
  return (
    <div className="min-h-screen bg-black text-white p-8 pb-20 sm:pt-20 px-8 pt-20 pb-14 font-[family-name:var(--font-geist-sans)]">
      <div className="top-0 text-white flex flex-col items-center space-y-2 py-4">
        <h1 className="text-3xl font-bold">Teams Page</h1>
        <h2 className="text-xl font-medium ">Coming soon</h2>

        <section className="text-white">
          <AccordionGroup sx={{ maxWidth: 400, color: "rgba(255, 255, 255)" }}>
            <Accordion>
              <AccordionSummary>First accordion</AccordionSummary>
              <AccordionDetails>
                Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do
                eiusmod tempor incididunt ut labore et dolore magna aliqua.
              </AccordionDetails>
            </Accordion>
            <Accordion>
              <AccordionSummary>Second accordion</AccordionSummary>
              <AccordionDetails>
                Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do
                eiusmod tempor incididunt ut labore et dolore magna aliqua.
              </AccordionDetails>
            </Accordion>
            <Accordion>
              <AccordionSummary>Third accordion</AccordionSummary>
              <AccordionDetails>
                Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do
                eiusmod tempor incididunt ut labore et dolore magna aliqua.
              </AccordionDetails>
            </Accordion>
          </AccordionGroup>
        </section>
      </div>
    </div>
  );
}
